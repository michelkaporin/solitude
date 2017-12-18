import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import ch.lubu.AvaData;
import ch.lubu.Chunk;

public class Main {
	private static int experimentReps = 1;
	private static int maxChunkSize;
	
	public static void main(String[] args) {
		maxChunkSize = Integer.valueOf(args[0]);
		experimentReps = Integer.valueOf(args[1]); // Fix HyperDex repetition amount for statistical confidence

		int[] maxBlockSize = { 1, maxChunkSize }; // Fix the block size per chunk
		boolean[] twoDimensions = { false, true }; // First run the retrieval from the key (single dimension), then over the second dimension

		AvaData avaData = new AvaData();
		SecretKey secretKey = generateSecretKey(); // Generate secret key for encrypted data representation
		HyperDex hd = new HyperDex();
		
		for (int blockSize : maxBlockSize) {
			for (boolean twoDimensional : twoDimensions) {
				List<Chunk> chunks = avaData.getChunks(blockSize, twoDimensional);
				
				// Optimise chunking, compression and encryption computations
				int totalSizeBase = 0;
				int totalSizeCompressed = 0;
				int totalSizeEncrypted = 0;
				for (Chunk chunk : chunks) {
					byte[] chunkedData = chunk.getData();
					byte[] cCompressedData = chunk.getCompressedData();
					byte[] ccEncryptedData = chunk.getCompressedAndEncryptedData(secretKey.getEncoded());
					totalSizeBase += chunkedData.length;
					totalSizeCompressed += cCompressedData.length;
					totalSizeEncrypted += ccEncryptedData.length;
				}
				System.out.format("\nBlock size per chunk: %s, Two Dimensional Benchmark: %s\n", blockSize,
						Boolean.toString(twoDimensional));
				System.out.format("Num Chunks: %d, Num Entries: %d\n", chunks.size(), avaData.counter);

				// Chunked data
				System.out.println(".:: Chunked Data ::.");
				BigDecimal avg = BigDecimal.valueOf(totalSizeBase).divide(BigDecimal.valueOf(chunks.size()), RoundingMode.HALF_UP);
				System.out.format("Total Size: %d, Average Chunk Size: %s\n", totalSizeBase, avg.toString());
				outputHyperdexGeneralStats(blockSize, chunks, hd, DataRepresentation.CHUNKED, twoDimensional, null);

				// Chunked & compressed data
				System.out.println(".:: Chunked & Compressed Data ::.");
				avg = BigDecimal.valueOf(totalSizeCompressed).divide(BigDecimal.valueOf(chunks.size()), RoundingMode.HALF_UP);
				System.out.format("Total Size: %d, Average Chunk Size: %s\n", totalSizeCompressed, avg.toString());
				outputHyperdexGeneralStats(blockSize, chunks, hd, DataRepresentation.CHUNKED_COMPRESSED, twoDimensional, null);

				// Chunked & compressed & encrypted data
				System.out.println(".:: Chunked & Compressed & Encrypted Data ::.");
				avg = BigDecimal.valueOf(totalSizeEncrypted).divide(BigDecimal.valueOf(chunks.size()), RoundingMode.HALF_UP);
				System.out.format("Total Size: %d, Average Chunk Size: %s\n", totalSizeEncrypted, avg.toString());
				outputHyperdexGeneralStats(blockSize, chunks, hd, DataRepresentation.CHUNKED_COMPRESSED_ENCRYPTED, twoDimensional,
						Optional.of(secretKey));
			}
		}
	}

	private static void outputHyperdexGeneralStats(int currentBlockSize, List<Chunk> chunks, HyperDex hd, DataRepresentation representation,
			boolean twodimensional, Optional<SecretKey> key) {
		for (int i = 0; i < experimentReps; i++) {
			hd.resetBenchmark();

			// Maintain map of chunks to delete because Ava dataset contains duplicates (entries with the same timestamp)
			Map<String, Chunk> chunksToDelete = new HashMap<String, Chunk>();
			
			// PUT
			for (Chunk chunk : chunks) {
				// Insert chunk to the HyperDex space
				boolean success = hd.put(chunk, representation, key, twodimensional);
				if (!success) {
					System.out.format("Inserting %s did not succeed\n", chunk.getPrimaryAttribute());
					continue;
				}
				chunksToDelete.put(chunk.getPrimaryAttribute(), chunk);
			}

			// GET
			for (Chunk chunk : chunks) {
				if (!twodimensional) {
					hd.get(chunk, representation);
				} else {
					hd.getSecond(representation, chunk.secondAttribute);
				}
			}

			System.out.format("[%s]\t%s\t%s\n", currentBlockSize, hd.getBenchmark().avgPut(), hd.getBenchmark().avgGet()); // Print PUT and GET average times
			
			// Range GET: prove that chunking is better than issuing range retrieval requests
			if (!twodimensional && currentBlockSize == 1) {
				for (int j=1; j < maxChunkSize; j++) {
					hd.resetBenchmark();
					Chunk c1 = chunks.get(0);
					Chunk c2 = chunks.get(j);
					hd.getRange(c1, c2, representation);
					System.out.format("[0..%s]\t%s\n", j, hd.getBenchmark().avgGet()); // Print PUT and GET average times
				}
			}

			// DEL
			for (Chunk chunk : chunksToDelete.values()) {
				Boolean deleted = hd.del(chunk, representation, twodimensional);
				if (!deleted || deleted == null) {
					System.out.format("Deleting %s did not succeed\n", chunk.getPrimaryAttribute());
				}
			}
		}
	}

	private static SecretKey generateSecretKey() {
		KeyGenerator keyGen = null;
		try {
			keyGen = KeyGenerator.getInstance("AES");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.out.println("Failed to obtain instance of AES");
		}
		
		keyGen.init(128); // for example
		
		return keyGen.generateKey();
	}
}