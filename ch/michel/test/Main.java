package ch.michel.test;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.crypto.SecretKey;

import ch.lubu.AvaData;
import ch.lubu.Chunk;
import ch.michel.DataRepresentation;
import ch.michel.HyperDex;
import ch.michel.Utility;

public class Main {
	private static int experimentReps = 1;
	private static int maxChunkSize;
	private static SecretKey secretKey = null;

	public static void main(String[] args) {
		maxChunkSize = Integer.valueOf(args[0]);
		experimentReps = Integer.valueOf(args[1]); // Fix HyperDex repetition amount for statistical confidence

		int[] maxBlockSize = { 1, maxChunkSize }; // Fix the block size per chunk
		boolean[] twoDimensions = { false, true }; // First run the retrieval from the key (single dimension), then over
													// the second dimension
		AvaData avaData = new AvaData();
		secretKey = Utility.generateSecretKey(); // Generate secret key for encrypted data representation
		HyperDex hd = new HyperDex();
		
		for (int blockSize : maxBlockSize) {
			for (boolean twoDimensional : twoDimensions) {
				List<Chunk> chunks = avaData.getChunks(blockSize, twoDimensional);

				// Optimise chunking, compression and encryption computations
				int totalSizeBase = 0;
				int totalSizeCompressed = 0;
				int totalSizeEncrypted = 0;
				for (Chunk chunk : chunks) {
					byte[] chunkedData = chunk.getData(DataRepresentation.CHUNKED, null);
					byte[] cCompressedData = chunk.getData(DataRepresentation.CHUNKED_COMPRESSED, null);
					byte[] ccEncryptedData = chunk.getData(DataRepresentation.CHUNKED_COMPRESSED_ENCRYPTED,
							Optional.of(secretKey));
					totalSizeBase += chunkedData.length;
					totalSizeCompressed += cCompressedData.length;
					totalSizeEncrypted += ccEncryptedData.length;
				}
				Utility.getTempLabels(chunks, 3);

				System.out.format("\nBlock size per chunk: %s, Two Dimensional Benchmark: %s\n", blockSize,
						Boolean.toString(twoDimensional));
				System.out.format("Num Chunks: %d, Num Entries: %d\n", chunks.size(), avaData.counter);

				// Chunked data
				System.out.println(".:: Chunked Data ::.");
				BigDecimal avg = BigDecimal.valueOf(totalSizeBase).divide(BigDecimal.valueOf(chunks.size()),
						RoundingMode.HALF_UP);
				System.out.format("Total Size: %d, Average Chunk Size: %s\n", totalSizeBase, avg.toString());
				 outputHyperdexGeneralStats(blockSize, chunks, hd, DataRepresentation.CHUNKED, twoDimensional);

				// Chunked & compressed data
				System.out.println(".:: Chunked & Compressed Data ::.");
				avg = BigDecimal.valueOf(totalSizeCompressed).divide(BigDecimal.valueOf(chunks.size()),
						RoundingMode.HALF_UP);
				System.out.format("Total Size: %d, Average Chunk Size: %s\n", totalSizeCompressed, avg.toString());
				 outputHyperdexGeneralStats(blockSize, chunks, hd, DataRepresentation.CHUNKED_COMPRESSED, twoDimensional);

				// Chunked & compressed & encrypted data
				System.out.println(".:: Chunked & Compressed & Encrypted Data ::.");
				avg = BigDecimal.valueOf(totalSizeEncrypted).divide(BigDecimal.valueOf(chunks.size()),
						RoundingMode.HALF_UP);
				System.out.format("Total Size: %d, Average Chunk Size: %s\n", totalSizeEncrypted, avg.toString());
				 outputHyperdexGeneralStats(blockSize, chunks, hd, DataRepresentation.CHUNKED_COMPRESSED_ENCRYPTED, twoDimensional);
			}
		}
	}

	private static void outputHyperdexGeneralStats(int currentBlockSize, List<Chunk> chunks, HyperDex hd,
			DataRepresentation representation, boolean twodimensional) {
		for (int i = 0; i < experimentReps; i++) {
			hd.resetBenchmark();

			// Maintain map of chunks to delete because Ava dataset contains duplicates (entries with the same timestamp)
			Map<String, Chunk> chunksToDelete = new HashMap<String, Chunk>();

			// PUT
			put(chunks, hd, representation, twodimensional, chunksToDelete);

			// GET
			get(chunks, hd, representation, twodimensional);

			System.out.format("[%s]\t%s\t%s\n", currentBlockSize, hd.getBenchmark().avgPut(),
					hd.getBenchmark().avgGet()); // Print PUT and GET average times

			// Range GET: prove that chunking is better than issuing range retrieval requests
			getRange(currentBlockSize, chunks, hd, representation, twodimensional);

			// DEL
			del(hd, representation, twodimensional, chunksToDelete);
		}
	}

	private static void del(HyperDex hd, DataRepresentation representation, boolean twodimensional,
			Map<String, Chunk> chunksToDelete) {
		String spaceName = Utility.getSpaceName(representation, twodimensional);

		for (Chunk chunk : chunksToDelete.values()) {
			Boolean deleted = hd.del(chunk, spaceName, twodimensional);
			if (!deleted || deleted == null) {
				System.out.format("Deleting %s did not succeed\n", chunk.getPrimaryAttribute());
			}
		}
	}

	private static void getRange(int currentBlockSize, List<Chunk> chunks, HyperDex hd,
			DataRepresentation representation, boolean twodimensional) {
		if (!twodimensional && currentBlockSize == 1) {
			String spaceName = Utility.getSpaceName(representation, false);
			for (int j = 1; j < maxChunkSize; j++) {
				hd.resetBenchmark();
				Chunk c1 = chunks.get(0);
				Chunk c2 = chunks.get(j);
				hd.getRange(c1, c2, spaceName);
				System.out.format("[0..%s]\t%s\n", j, hd.getBenchmark().avgGet()); // Print PUT and GET average times
			}
		}
	}

	private static void get(List<Chunk> chunks, HyperDex hd, DataRepresentation representation,
			boolean twodimensional) {
		String spaceName = Utility.getSpaceName(representation, twodimensional);
		for (Chunk chunk : chunks) {
			if (!twodimensional) {
				hd.get(chunk, spaceName);
			} else {
				hd.getSecond(spaceName, chunk.secondAttribute);
			}
		}
	}

	private static void put(List<Chunk> chunks, HyperDex hd, DataRepresentation representation, boolean twodimensional, Map<String, Chunk> chunksToDelete) {
		String spaceName = Utility.getSpaceName(representation, twodimensional);
		for (Chunk chunk : chunks) {
			// Insert chunk to the HyperDex space
			byte[] data = chunk.getData(representation, Optional.of(secretKey));
			boolean success = hd.put(chunk, spaceName, data, twodimensional);
			if (!success) {
				System.out.format("Inserting %s did not succeed\n", chunk.getPrimaryAttribute());
				continue;
			}
			chunksToDelete.put(chunk.getPrimaryAttribute(), chunk);
		}
	}
}