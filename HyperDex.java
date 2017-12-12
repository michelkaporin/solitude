
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.crypto.SecretKey;

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

	public boolean put(Chunk chunk, DataRepresentation state, Optional<SecretKey> secretKey, boolean twodimensional) {
		Map<String, Object> attributes = new HashMap<String, Object>();

		String space = getSpaceName(state, twodimensional);
		byte[] data = null;
		SecretKey key = null;

		switch (state) {
			case CHUNKED_COMPRESSED:
				data = chunk.getCompressedData();
				break;
			case CHUNKED_COMPRESSED_ENCRYPTED:
				if (secretKey.isPresent()) {
					key = secretKey.get();
				} else {
					break;
				}
	
				try {
					data = chunk.getCompressedAndEncryptedData(key.getEncoded());
				} catch (Exception e1) {
					e1.printStackTrace();
					System.out.println("Failed to obtain secret key");
				}
				break;
			default:
				data = chunk.getData();
				break;
		}

		if (data == null) {
			return false;
		}

		attributes.put(DATA_ATTRIBUTE_NAME, new String(data));
		if (twodimensional) {
			attributes.put(SECOND_ATTRIBUTE_NAME, String.valueOf(chunk.secondAttribute)); // second attribute
		}

		try {
			long start = System.nanoTime();
			boolean res = client.put(space, chunk.getPrimaryAttribute(), attributes);
			benchmark.addPutRequestTime(System.nanoTime() - start);
			return res;
		} catch (HyperDexClientException e) {
			e.printStackTrace();
			return false;
		}
	}

	public Map<String, Object> get(Chunk chunk, DataRepresentation representation) {
		Map<String, Object> res = null;
		long start = System.nanoTime();
		try {
			res = client.get(this.getSpaceName(representation, false), chunk.getPrimaryAttribute());
		} catch (HyperDexClientException e) {
			System.out.format("Retrieving %s did not succeed\n", chunk.getPrimaryAttribute());
			e.printStackTrace();
		}
		benchmark.addGetRequestTime(System.nanoTime() - start);

		return res;
	}

	public Boolean del(Chunk chunk, DataRepresentation representation, boolean twodimensional) {
		try {
			Object res = client.del(this.getSpaceName(representation, twodimensional), chunk.getPrimaryAttribute());
			if (res == null) {
				return false;
			}

			return (Boolean) res;
		} catch (HyperDexClientException e) {
			e.printStackTrace();
			return false;
		}
	}

	public Iterator getSecond(DataRepresentation representation, Object secondAttribute) {
		String spaceName = this.getSpaceName(representation, true);

		Map<String, Object> predicates = new HashMap<String, Object>();
		predicates.put(SECOND_ATTRIBUTE_NAME, String.valueOf(secondAttribute));

		long start = System.nanoTime();
		Iterator it = client.search(spaceName, predicates);
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

		return null;
	}
	
	public Iterator getRange(Chunk c1, Chunk c2, DataRepresentation representation) {
		String spaceName = this.getSpaceName(representation, false);

		Map<String, Object> predicates = new HashMap<String, Object>();
		predicates.put(FIRST_ATTRIBUTE_NAME, new Range(c1.getPrimaryAttribute(), c2.getPrimaryAttribute()));
		
		long start = System.nanoTime();
		Iterator it = client.search(spaceName, predicates);
		try {
			if (it.hasNext()) {
				benchmark.addGetRequestTime(System.nanoTime() - start);
				return it;
			} else {
				System.out.format("No results for the range [%s, %s]", c1.getPrimaryAttribute(), c2.getPrimaryAttribute()); // can provide range myself.
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

	private String getSpaceName(DataRepresentation representation, boolean twodimensional) {
		switch (representation) {
		case CHUNKED_COMPRESSED:
			if (twodimensional) {
				return "compressed_c2";
			}
			return "compressed_c";
		case CHUNKED_COMPRESSED_ENCRYPTED:
			if (twodimensional) {
				return "encrypted_cc2";
			}
			return "encrypted_cc";
		default:
			if (twodimensional) {
				return "chunked2";
			}
			return "chunked";
		}
	}
}
