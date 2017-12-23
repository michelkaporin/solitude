package ch.michel;

import java.util.HashMap;
import java.util.Map;

import org.hyperdex.client.Client;
import org.hyperdex.client.HyperDexClientException;
import org.hyperdex.client.Iterator;
import org.hyperdex.client.Range;

import ch.lubu.Chunk;

public class HyperDex {

	private Client client;
	private Benchmark benchmark;

	private static String DEFAULT_IP = "127.0.0.1";
	private static int DEFAULT_PORT = 1982;
	private static String DATA_ATTRIBUTE_NAME = "data";
	private static String FIRST_ATTRIBUTE_NAME = "time";
	private static String SECOND_ATTRIBUTE_NAME = "temp_skin";

	public HyperDex() {
		this.client = new Client(DEFAULT_IP, DEFAULT_PORT);
		this.benchmark = new Benchmark();
	}

	public HyperDex(String ip, int port) {
		this.client = new Client(ip, port);
		this.benchmark = new Benchmark();
	}

	public boolean put(Chunk chunk, String space, byte[] data, boolean twodimensional) {
		Map<String, Object> attributes = new HashMap<String, Object>();

		if (data == null) {
			return false;
		}

		attributes.put(DATA_ATTRIBUTE_NAME, new String(data));
		if (twodimensional) {
			attributes.put(SECOND_ATTRIBUTE_NAME, chunk.secondAttribute); // second attribute
		}

		try {
			long start = System.nanoTime();
			boolean res = client.put(space, chunk.getPrimaryAttribute(), attributes);
			benchmark.addPutRequestTime(System.nanoTime() - start);
			return res;
		} catch (HyperDexClientException e) {
			e.printStackTrace();
			System.exit(1);
			return false;
		}
	}

	public Map<String, Object> get(Chunk chunk, String space) {
		Map<String, Object> res = null;
		long start = System.nanoTime();
		try {
			res = client.get(space, chunk.getPrimaryAttribute());
		} catch (HyperDexClientException e) {
			System.out.format("Retrieving %s did not succeed\n", chunk.getPrimaryAttribute());
			e.printStackTrace();
		}
		benchmark.addGetRequestTime(System.nanoTime() - start);

		return res;
	}
	
	public Boolean del(Chunk chunk, String space, boolean twodimensional) {
		try {
			Object res = client.del(space, chunk.getPrimaryAttribute());
			if (res == null) {
				return false;
			}

			return (Boolean) res;
		} catch (HyperDexClientException e) {
			e.printStackTrace();
			return false;
		}
	}

	public Iterator getSecond(String space, int secondAttribute) {
		Map<String, Object> predicates = new HashMap<String, Object>();
		predicates.put(SECOND_ATTRIBUTE_NAME, secondAttribute);

		long start = System.nanoTime();
		Iterator it = client.search(space, predicates);
		try {
			if (it.hasNext()) {
				benchmark.addGetRequestTime(System.nanoTime() - start);
				return it;
			} else {
				System.out.println("No results for " + String.valueOf(secondAttribute));
			}
		} catch (HyperDexClientException e) {
			e.printStackTrace();
			System.out.println("Interaction with HyperDex failed");
		}

		benchmark.addGetRequestTime(System.nanoTime() - start);
		return null;
	}
	
	public Iterator getRange(Chunk c1, Chunk c2, String space) {
		Map<String, Object> predicates = new HashMap<String, Object>();
		predicates.put(FIRST_ATTRIBUTE_NAME, new Range(c1.getPrimaryAttribute(), c2.getPrimaryAttribute()));
		
		long start = System.nanoTime();
		Iterator it = client.search(space, predicates);
		try {
			if (it.hasNext()) {
				benchmark.addGetRequestTime(System.nanoTime() - start);
				return it;
			} else {
				System.out.format("No results for the range [%s, %s]", c1.getPrimaryAttribute(), c2.getPrimaryAttribute());
			}
		} catch (HyperDexClientException e) {
			e.printStackTrace();
			System.out.println("Interaction with HyperDex failed");
		}

		return null;
	}

	public Iterator getTempRange(int temp1, int temp2, String space, int rangeLimit) {
		Map<String, Object> predicates = new HashMap<String, Object>();
		predicates.put(SECOND_ATTRIBUTE_NAME, new Range(Integer.valueOf(temp1), Integer.valueOf(temp2)));
		
		int count = 0;
		long start = System.nanoTime();
		Iterator it = client.search(space, predicates);
		try {
			if (it.hasNext()) {
				// Two ways to benchmark time: right after hasNext() or go through all elements and then benchmark
				while (it.hasNext() && count <= rangeLimit) {
					it.next();
					count++;
				}
				benchmark.addGetRequestTime(System.nanoTime() - start);
				return it;
			} else {
				System.out.format("No results for the range [%s, %s]", temp1, temp2);
			}
		} catch (HyperDexClientException e) {
			e.printStackTrace();
			System.out.println("Interaction with HyperDex failed");
		}
		
		return null;
	}
	
	public Benchmark getBenchmark() {
		return this.benchmark;
	}
	
	public void resetBenchmark() {
		this.benchmark = new Benchmark();
	}
}
