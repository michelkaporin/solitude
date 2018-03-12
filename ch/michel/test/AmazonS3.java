package ch.michel.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.crypto.SecretKey;


import ch.lubu.AvaData;
import ch.lubu.Chunk; 
import ch.michel.DataRepresentation;
import ch.michel.HyperDex;
import ch.michel.S3;
import ch.michel.Utility;

public class AmazonS3 {
	private static int experimentReps = 1;
	private static int maxChunkSize;
	private static SecretKey secretKey = null;

	public static void main(String[] args) {
		System.out.println("Compare encrypted and compressed chunks stored in HyperDex vs. Amazon S3");
		
		maxChunkSize = Integer.valueOf(args[0]);
		experimentReps = Integer.valueOf(args[1]); // Fix HyperDex repetition amount for statistical confidence
		String hyperdexIP = args[2];
		String aws_access_key_id = args[3];
		String aws_secret_access_key = args[4];
		
		AvaData avaData = new AvaData();
		HyperDex hd = new HyperDex(hyperdexIP, 1982);
		S3 s3 = new S3(aws_access_key_id, aws_secret_access_key);
		
		secretKey = Utility.generateSecretKey(); // Generate secret key for encrypted data representation
		DataRepresentation dr = DataRepresentation.CHUNKED_COMPRESSED_ENCRYPTED;
		String bucket = "hyperdex";
		
		List<Chunk> chunks = avaData.getChunks(maxChunkSize, false, false, false);
		Map<String, Chunk> chunksToDelete = new HashMap<String, Chunk>();

		for (int i = 0; i < experimentReps; i++) {
			String spaceName = Utility.getSpaceName(dr, false);

			// HYPERDEX
			// PUT
			for (Chunk chunk : chunks) {
				byte[] data = chunk.serialise(dr, Optional.of(secretKey));
				boolean success = hd.put(chunk, spaceName, data, false);
				if (!success) {
					System.out.println("Failed to put chunk " + chunk.getPrimaryAttribute());
				}
				chunksToDelete.put(chunk.getPrimaryAttribute(), chunk);
			}

			// GET
			for (Chunk chunk : chunks) {
				hd.get(chunk, spaceName);
			}
			System.out.format("HyperDex: [%s]\t%s\t%s\n", maxChunkSize, hd.getBenchmark().avgPut(), hd.getBenchmark().avgGet());

			// DEL
			for (Chunk chunk : chunksToDelete.values()) {
				hd.del(chunk, spaceName, false);
			}
			
			// AMAZON S3
			// PUT
			for (Chunk chunk : chunks) {
				byte[] data = chunk.serialise(dr, Optional.of(secretKey));
				s3.put(chunk, bucket, data);
			}

			// GET
			for (Chunk chunk : chunks) {
				s3.get(chunk, bucket);
			}
			System.out.format("Amazon: [%s]\t%s\t%s\n", maxChunkSize, s3.getBenchmark().avgPut(), s3.getBenchmark().avgGet());

			// DEL
			for (Chunk chunk : chunksToDelete.values()) {
				s3.del(chunk, bucket);
			}
		}
	}
	
}
