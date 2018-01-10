package ch.michel;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import ch.lubu.Chunk;

public class Cassandra implements Storage {

	private Cluster cluster;
    private Session session;
    
    private Benchmark benchmark;
    
	public Cassandra(String ip, int port) {
		this.connect(ip, port);
		this.createKeyspace("main", 2);
		this.benchmark = new Benchmark();
	}
	
	public void connect(String node, int port) {
        Builder b = Cluster.builder().addContactPoint(node).withPort(port);
        this.cluster = b.build();
        this.session = cluster.connect();
    }
 
    public Session getSession() {
        return this.session;
    }
 
    public void close() {
        session.close();
        cluster.close();
    }
    
    public boolean put(Chunk chunk, String table, byte[] data) {
		StringBuilder sb = new StringBuilder("INSERT INTO ")
		.append(table).append("(key, value) ")
		.append("VALUES (").append(chunk.getPrimaryAttribute())
		.append(", '").append(data).append("');");
		
		String query = sb.toString();

		long start = System.nanoTime();
		session.execute(query);
		benchmark.addPutRequestTime(System.nanoTime() - start);
		
		return true;
    	}
    
    public List<byte[]> getAll(String table) {
    		List<byte[]> chunks = new ArrayList<byte[]>();
    		
	    	StringBuilder sb = new StringBuilder("SELECT * FROM ").append(table);
	    	String query = sb.toString();
	    	
		long start = System.nanoTime();
	    	ResultSet rs = session.execute(query);
	
	    	rs.forEach(r -> {
	    		ByteBuffer buffer = r.getBytes("value");
	    		byte[] res = new byte[buffer.remaining()];
	    		buffer.get(res);
	    		chunks.add(res);
	    	});
		benchmark.addGetRequestTime(System.nanoTime() - start);
	    	
		return chunks;
    	}
    

    public boolean del(Chunk chunk, String table) {
		StringBuilder sb = new StringBuilder("DELETE FROM ")
		.append(table)
		.append("WHERE key = '").append(chunk.getPrimaryAttribute())
		.append("';");
		String query = sb.toString();

		session.execute(query);
		return true;
    	}
    
    public void createKeyspace(String keyspaceName, int replicationFactor) {
			StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
	      .append(keyspaceName).append(" WITH replication = {")
	      .append("'class':'").append("SimpleStrategy") // Replication strategy class
	      .append("','replication_factor':").append(replicationFactor)
	      .append("};");
	         
	    String query = sb.toString();
	    session.execute(query);
	}
    
    public void createTable(String tableName) {
		StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
			.append(tableName).append("(")
			.append("key timestamp PRIMARY KEY, ")
			.append("value blob);");
		String query = sb.toString();
		session.execute(query);
    	}

	@Override
	public Benchmark getBenchmark() {
		return this.benchmark;
	}

	@Override
	public void resetBenchmark() {
		this.benchmark = new Benchmark();
	}
}
