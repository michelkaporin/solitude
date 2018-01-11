package ch.michel;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import ch.lubu.Chunk;

public class Cassandra implements Storage {

	private static String KEYSPACE = "main";
	
	private Cluster cluster;
    private Session session;
    
    private Benchmark benchmark;
    
	public Cassandra(String ip, int port) {
		this.connect(ip, port);
		this.createKeyspace(2);
		this.benchmark = new Benchmark();
	}
	
	public void connect(String node, int port) {
        Builder b = Cluster.builder().addContactPoint(node).withPort(port);
        this.cluster = b.build();
        try {
        		this.session = cluster.connect();
        } catch (Exception e) {
        		System.out.println("Failed to connect to Cassandra:\n" + e.toString());
        		System.exit(1);
        }
    }
 
    public Session getSession() {
        return this.session;
    }
 
    public void close() {
        session.close();
        cluster.close();
    }
    
    public boolean put(Chunk chunk, String table, byte[] data) {
	    	ByteBuffer buffer = ByteBuffer.wrap(data);

	    	Date time = new Date(Long.valueOf(chunk.getPrimaryAttribute()));
	
	    String insertStm = String.format("INSERT INTO %s.%s (key, value) VALUES (?,?)", KEYSPACE, table);
	    	PreparedStatement ps = session.prepare(insertStm);
	    	BoundStatement boundStatement = new BoundStatement(ps);
	    	Statement stm = boundStatement.bind(time, buffer);

	    	try {
			long start = System.nanoTime();
		    	session.execute(stm);
			benchmark.addPutRequestTime(System.nanoTime() - start);
	    	} catch (Exception e) {
	    		return false;
	    	}
		return true;
    	}
    
    public List<byte[]> getAll(String table) {
    		List<byte[]> chunks = new ArrayList<byte[]>();
    		
    		String stm = String.format("SELECT * FROM %s.%s", KEYSPACE, table);
	    	
		long start = System.nanoTime();
	    	ResultSet rs = session.execute(stm);
	
	    	rs.forEach(r -> {
	    		ByteBuffer buffer = r.getBytes("value");
	    		chunks.add(buffer.array());
	    	});
		benchmark.addGetRequestTime(System.nanoTime() - start);
	    	
		return chunks;
    	}

    public boolean del(Chunk chunk, String table) {
    		String stm = String.format("DELETE FROM %s.%s WHERE key = '%s'", KEYSPACE, table, chunk.getPrimaryAttribute());
    		try {
    			session.execute(stm);
    		} catch (Exception e) {
    			System.out.println("Failed to delete chunk: " + e);
    			return false;
    		}
    		
		return true;
    	}
    
    public void delAll(String table) {
    		String stm = String.format("TRUNCATE %s.%s", KEYSPACE, table);
    		session.execute(stm);
    }
    
    public void createKeyspace(int replicationFactor) {
			StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
	      .append(KEYSPACE).append(" WITH replication = {")
	      .append("'class':'").append("SimpleStrategy") // Replication strategy class
	      .append("','replication_factor':").append(replicationFactor)
	      .append("};");
	         
	    String query = sb.toString();
	    session.execute(query);
	}
    
    public void createTable(String tableName) {
	    	try {
	    		String stm = String.format("CREATE TABLE IF NOT EXISTS %s.%s (key timestamp PRIMARY KEY, value blob);", KEYSPACE, tableName);
			session.execute(stm);
	    	} catch (Exception e) {
        		System.out.println("Failed to create table in Cassandra:\n" + e.toString());
        		System.exit(1);
	    	}
    	}

	@Override
	public Benchmark getBenchmark() {
		return this.benchmark;
	}

	@Override
	public void resetBenchmark() {
		this.benchmark = new Benchmark();
	}

	public void createTables(List<Label> labels) {
		for (Label label : labels) {
			this.createTable(label.name);
		}
	}
	public void deleteTableRecords(List<Label> labels) {
		for (Label label : labels) {
			this.delAll(label.name);
		}
	}
}
