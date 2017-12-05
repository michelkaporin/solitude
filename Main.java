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
	public static void main(String[] args) {
		System.out.println("Hello, Solitude!");
		AvaData avaData = new AvaData();
		int[] maxBlockSize = { 1, 1000 }; // Fix the block size per chunk
		boolean[] twoDimensions = { false, true }; // First run the retrieval from the key (single dimension), then over
													// the second dimension
		int experimentReps = 3; // Fix HyperDex repetition amount for statistical confidence

		SecretKey secretKey = generateSecretKey(); // Generate secret key for encrypted data representation
		
		for (int blockSize : maxBlockSize) {
			List<Chunk> chunks = avaData.transferData(blockSize);

			for (boolean twoDimensional : twoDimensions) {
				System.out.format("Block size per chunk: %s, Two Dimensional Benchmark: %s\n", blockSize,
						Boolean.toString(twoDimensional));
				System.out.format("Num Chunks: %d, Num Entries: %d\n", chunks.size(), avaData.counter);

				// Chunked data
				System.out.println(".:: Chunked Data ::.");
				int totalSizeBase = 0;
				for (Chunk chunk : chunks) {
					byte[] data = chunk.getData();
					totalSizeBase += data.length;
				}
				BigDecimal avg = BigDecimal.valueOf(totalSizeBase).divide(BigDecimal.valueOf(chunks.size()), RoundingMode.HALF_UP);
				System.out.format("Total Size: %d, Average Chunk Size: %s\n", totalSizeBase, avg.toString());
				outputHyperdexStats(experimentReps, chunks, DataRepresentation.CHUNKED, twoDimensional, null);

				// Chunked & compressed data
				System.out.println(".:: Chunked & Compressed Data ::.");
				int totalSize = 0;
				for (Chunk block : chunks) {
					byte[] data = block.getCompressedData();
					totalSize += data.length;
				}
				avg = BigDecimal.valueOf(totalSize).divide(BigDecimal.valueOf(chunks.size()), RoundingMode.HALF_UP);
				System.out.format("Total Size: %d, Average Chunk Size: %s\n", totalSize, avg.toString());
				outputHyperdexStats(experimentReps, chunks, DataRepresentation.CHUNKED_COMPRESSED, twoDimensional, null);

				// Chunked & compressed & encrypted data
				System.out.println(".:: Chunked & Compressed & Encrypted Data ::.");
				totalSize = 0;
				for (Chunk chunk : chunks) {
					byte[] data = chunk.getCompressedAndEncryptedData(secretKey.getEncoded());
					totalSize += data.length;
				}
				avg = BigDecimal.valueOf(totalSize).divide(BigDecimal.valueOf(chunks.size()), RoundingMode.HALF_UP);
				System.out.format("Total Size: %d, Average Chunk Size: %s\n", totalSize, avg.toString());
				outputHyperdexStats(experimentReps, chunks, DataRepresentation.CHUNKED_COMPRESSED_ENCRYPTED, twoDimensional,
						Optional.of(secretKey));
			}
		}
	}

	private static void outputHyperdexStats(int experimentReps, List<Chunk> chunks, DataRepresentation representation,
			boolean twodimensional, Optional<SecretKey> key) {
		for (int i = 0; i < experimentReps; i++) {
			HyperDex hd = new HyperDex();

			// Maintain map of chunks to delete because Ava Dataset contains duplicates (entries with the same timestamp)
			Map<Long, Chunk> chunksToDelete = new HashMap<Long, Chunk>(); 
			
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

			System.out.format("%s\t%s\n", hd.getBenchmark().avgPut(), hd.getBenchmark().avgGet()); // Print PUT and GET average times

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