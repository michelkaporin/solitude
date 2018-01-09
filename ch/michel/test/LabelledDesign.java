package ch.michel.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.crypto.SecretKey;

import ch.lubu.AvaData;
import ch.lubu.Chunk;
import ch.michel.DataRepresentation;
import ch.michel.HyperDex;
import ch.michel.Label;
import ch.michel.Utility;

public class LabelledDesign {
	private static int experimentReps = 1;
	private static int maxChunkSize;
	private static SecretKey secretKey = null;
	private static DataRepresentation[] dataRepresentations = new DataRepresentation[] { 
			DataRepresentation.CHUNKED, DataRepresentation.CHUNKED_COMPRESSED, DataRepresentation.CHUNKED_COMPRESSED_ENCRYPTED 
			};

	public static void main(String[] args) {
		maxChunkSize = Integer.valueOf(args[0]);
		experimentReps = Integer.valueOf(args[1]); // Fix HyperDex repetition amount for statistical confidence

		AvaData avaData = new AvaData();
		secretKey = Utility.generateSecretKey(); // Generate secret key for encrypted data representation
		HyperDex hd = new HyperDex();

		List<Chunk> chunks = avaData.getChunks(maxChunkSize, false);
		List<Chunk> plainEntries = avaData.getChunks(1, false);
		List<Label> labels = Utility.getTempLabels(chunks, 3);

		// Create HyperSpaces in HyperDex
		hd.createSpaces(labels);

		Map<String, Chunk> chunksToDelete = new HashMap<String, Chunk>();
		Map<String, Chunk> plainEntriesToDelete = new HashMap<String, Chunk>();

		System.out.println("Chunks: " + chunks.size());
		System.out.println("Plaintext entries: " + plainEntries.size());

		for (int i = 0; i < experimentReps; i++) {
			System.out.format("\n%s)\n", i+1);
			for (DataRepresentation dr : dataRepresentations) {
				System.out.println("Labelled data stored as " + dr.toString());
				hd.resetBenchmark();
	
				// 1. Benchmark storing chunks in labels
				// PUT
				for (Chunk chunk : chunks) {
					byte[] data = chunk.serialise(dr, Optional.of(secretKey));
					String spaceName = getLabelName(labels, chunk);
	
					boolean success = hd.put(chunk, spaceName, data, false);
					if (!success) {
						System.out.println("Failed to put chunk " + chunk.getPrimaryAttribute());
					}
					chunksToDelete.put(chunk.getPrimaryAttribute(), chunk);
				}
	
				// GET
				for (Chunk chunk : chunks) {
					String spaceName = getLabelName(labels, chunk);
					hd.get(chunk, spaceName);
				}
				System.out.format("[%s]\t%s\t%s\n", maxChunkSize, hd.getBenchmark().avgPut(), hd.getBenchmark().avgGet());
	
				// DEL
				for (Chunk chunk : chunksToDelete.values()) {
					String spaceName = getLabelName(labels, chunk);
					hd.del(chunk, spaceName, false);
				}
	
				// 2. Benchmark ranges retrieval
				// PUT all chunks in a normal table
				String spaceName = Utility.getSpaceName(dr, true);
				for (Chunk plainEntry : plainEntries) {
					// Insert chunk to the HyperDex space
					byte[] data = plainEntry.serialise(dr, Optional.of(secretKey));
					boolean success = hd.put(plainEntry, spaceName, data, true);
					if (!success) {
						System.out.format("Inserting %s did not succeed\n", plainEntry.getPrimaryAttribute());
						continue;
					}
					plainEntriesToDelete.put(plainEntry.getPrimaryAttribute(), plainEntry);
				}
				// GET chunks by ranges provided in labels
				for (Label l : labels) {
					hd.resetBenchmark();
					// Average out the time by repeating range gets 
					for (int j=0; j < experimentReps; j++) {
						hd.getTempRange(l.low, l.high, spaceName, maxChunkSize);
					}
					System.out.format("[%s..%s]\t%s\n", l.low, l.high, hd.getBenchmark().avgGet()); // Print PUT and GET
																									// average times
				}
				// DEL
				for (Chunk chunk : plainEntriesToDelete.values()) {
					hd.del(chunk, spaceName, false);
				}
			}
		}
	}

	private static String getLabelName(List<Label> labels, Chunk c) {
		for (Label l : labels) {
			if (c.secondAttribute >= l.low && c.secondAttribute <= l.high) {
				return l.name;
			}
		}

		throw new NullPointerException("No label was found for chunk temp_skin: " + c.secondAttribute);
	}
}
