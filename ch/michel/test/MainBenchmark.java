package ch.michel.test;

import ch.lubu.AvaData;
import ch.lubu.Chunk;
import ch.michel.DataRepresentation;
import ch.michel.HyperDex;
import ch.michel.Label;
import ch.michel.S3;
import ch.michel.Utility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.crypto.SecretKey;

import org.hyperdex.client.ByteString;

/*
 * 0. For each of underlying structures (HyperDex, S3, Cassandra) do:
 * 	0.1 Baseline design data retrieval test
 * 	0.2 Labelled design data retrieval test
 * 
 * 	BASELINE
 * 	HyperDex:
 * 	1. Range query search
 * 		1.1 For chunked, cc, cc&e:
 * 			1.1.1 Store in DB [timestamp]
 * 			1.1.2 Retrieve from DB [timestamp]
 * 			1.1.3 Decompress/decrypt where needed [timestamp]
 * 	(2). Get all *Optional
 * 		1.1 For chunked, cc, cc&e:
 * 			1.1.1 Store in DB [timestamp]
 * 			1.1.2 Retrieve from DB [timestamp]
 * 			1.1.3 Decompress/decrypt where needed [timestamp]
 * 
 * 	S3, Cassandra:
 * 	1. Get all as in HyperDex
 * 		1.1 For chunked, cc, cc&e:
 * 			1.1.1 Store in DB [timestamp]
 * 			1.1.2 Retrieve from DB [timestamp]
 * 			1.1.3 Decompress/decrypt where needed [timestamp]
 *
 * 
 * 	LABELLED DESIGN COMMON FOR ALL UNDERLYING STORAGES
 * 	1. Primary-key search
 * 		1.1 For chunked, cc, cc&e:
 * 			1.1.1 Store in DB [timestamp]
 * 			1.1.2 Retrieve from DB [timestamp]
 * 			1.1.3 Decompress/decrypt where needed [timestamp]
 * 
 */
public class MainBenchmark {
	private static DataRepresentation[] dataRepresentations = new DataRepresentation[] { DataRepresentation.CHUNKED,
			DataRepresentation.CHUNKED_COMPRESSED, DataRepresentation.CHUNKED_COMPRESSED_ENCRYPTED };

	public static void main(String[] args) {
		int maxChunkSize = Integer.valueOf(args[0]);
		int experimentReps = Integer.valueOf(args[1]); // Fix HyperDex repetition amount for statistical confidence
		String hyperdexIP = args[2];
		String aws_access_key_id = args[3];
		String aws_secret_access_key = args[4];
		String bucket = "solitude-baseline";

		System.out.println("BASELINE");
		System.out.format("%s\t%s\t%s\t%s\t%s\t%s\n", "Data Store", "Representation", "PUT", "GET", "Decrypt & Decompress", "Search");
		AvaData avaData = new AvaData();
		SecretKey secretKey = Utility.generateSecretKey();
		List<Chunk> chunks = avaData.getChunks(maxChunkSize, false);
		List<Label> labels = Utility.getTempLabels(chunks, 3); // obtain labels for produced chunks

		HyperDex hd = new HyperDex(hyperdexIP, 1982);
		S3 s3 = new S3(aws_access_key_id, aws_secret_access_key);
		
		Map<String, Chunk> chunksToDelete = new HashMap<String, Chunk>();
		for (DataRepresentation dr : dataRepresentations) {
			// PUT 
			String space = Utility.getSpaceName(dr, true);
			for (Chunk chunk : chunks) {
				byte[] data = chunk.serialise(dr, Optional.of(secretKey));
				boolean success1 = hd.put(chunk, space, data, true);
				boolean success2 = s3.put(chunk, bucket, data);
				if (!success1 || !success2) {
					System.out.println("Failed to put chunk in either HyperDex or S3" + chunk.getPrimaryAttribute());
					System.exit(1);
				}
				chunksToDelete.put(chunk.getPrimaryAttribute(), chunk);
			}
			
			// GET
			// 1. HyperDex GET
			Collection<ByteString> hdResults = hd.getTempRange(labels.get(0).low, labels.get(0).high, space, chunks.size());
			
			// 2. S3 GET
			Collection<byte[]> s3Results = new ArrayList<byte[]>();
			for (Chunk chunk : chunks) {
				s3Results.add(s3.get(chunk, bucket));
			}
			
			// Deserialise, decompress & decrypt if needed
			// 1. HyperDex Objects
			long start = System.nanoTime();
			for (ByteString res: hdResults) {
				Chunk.deserialise(dr, res.getBytes(), Optional.of(secretKey));
			}
			long hdDecodeElapsed = (System.nanoTime() - start)/1000000; // maybe more detailed breakdown for decompression and decryption?
			
			// 2. S3 Objects: retrieve all of them and then search for the range locally
			start = System.nanoTime();
			for (byte[] res: s3Results) {
				Chunk.deserialise(dr, res, Optional.of(secretKey));
			}
			long s3DecodeElapsed = (System.nanoTime() - start)/1000000;
			
			// Query for the range of chunks
			Collection<Chunk> chunksInRange = new ArrayList<Chunk>();
			start = System.nanoTime();
			for (byte[] res: s3Results) {
				Chunk c = Chunk.deserialise(dr, res, Optional.of(secretKey));
				if (c.secondAttribute > labels.get(0).low && c.secondAttribute < labels.get(0).high) {
					chunksInRange.add(c);
				}
			}
			long avgSearch = (System.nanoTime() - start)/1000000;

			printStats("HyperDex", dr, hd.getBenchmark().avgPut(), hd.getBenchmark().avgGet(), hdDecodeElapsed, 0); // hyperdex
			printStats("S3", dr, s3.getBenchmark().avgPut(), s3.getBenchmark().avgGet(), s3DecodeElapsed, avgSearch); // s3
			
			// Clean the state -> DEL items from the store, reset benchmarks
			// DEL
			for (Chunk chunk : chunks) {
				hd.del(chunk, space, true);
				s3.del(chunk, bucket);
			}
			hd.resetBenchmark();
			s3.resetBenchmark();
		}
		
	}

	private static void printStats(String datastore, DataRepresentation dr, double avgPut, double avgGet, double avgDecode, double avgSearch) {
		System.out.format("%s\t%s\t%s\t%s\t%s\t%s\n", datastore, dr, avgPut, avgGet, avgDecode, avgSearch);
	}
}
