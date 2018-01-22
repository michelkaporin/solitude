package ch.michel.test;

import ch.lubu.AvaData;
import ch.lubu.Chunk;
import ch.michel.Cassandra;
import ch.michel.DataRepresentation;
import ch.michel.HyperDex;
import ch.michel.Label;
import ch.michel.S3;
import ch.michel.Utility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
		String cassandraIP = args[5];
		int hyperdexPort = 1982;
		int cassandraPort = 9042;
		
		// Setup phase
		AvaData avaData = new AvaData();
		SecretKey secretKey = Utility.generateSecretKey();
		List<Chunk> chunks = avaData.getChunks(maxChunkSize, false);
		List<Chunk> singleEntryChunks = avaData.getChunks(1, false);
		List<Label> labels = Utility.getTempLabels(chunks, 3); // obtain labels for produced chunks
		
		HyperDex hd = new HyperDex(hyperdexIP, hyperdexPort);
		S3 s3 = new S3(aws_access_key_id, aws_secret_access_key);
		Cassandra minicrypt = new Cassandra(cassandraIP, cassandraPort);
		Cassandra cassSingle = new Cassandra(cassandraIP, cassandraPort);
		String bucket = "solitude-baseline";
		String cassandraTable = "baseline";
		String cassandraSingleRecordTable = "baselineSingle";
		
		hd.createSpaces(labels);
		s3.createBuckets(labels);		
		minicrypt.createTable(cassandraTable);
		cassSingle.createSingleEntryTable(cassandraSingleRecordTable);
		minicrypt.delAll(cassandraTable); // wipe the table if any records left from previous executions
		cassSingle.delAll(cassandraSingleRecordTable);
		minicrypt.createTables(labels);
		minicrypt.deleteTableRecords(labels);
		
		System.out.format("%s\t%s\t%s\t%s\t%s\t%s\n", "Data Store", "Representation", "PUT", "GET", "Deserialise, decrypt & Decompress", "Search");
		for (int i = 0; i < experimentReps; i++) {
			/*
			 * Baseline Design Benchmark
			 */
			System.out.println("BASELINE");
		
			for (DataRepresentation dr : dataRepresentations) {
				// PUT single entries in HyperDex (does not make sense putting chunks because no way to identify a chunk grouped by time with the second dimension)
				String space = Utility.getSpaceName(dr, true);
				for (Chunk chunk : singleEntryChunks) {
					byte[] data = chunk.serialise(dr, Optional.of(secretKey));
					boolean success1 = hd.put(chunk, space, data, true);
					boolean success2 = cassSingle.put(chunk, cassandraSingleRecordTable, data, true); // Adding cassandra entry-by-entry
					if (!success1 && !success2) {
						System.out.println("Failed to put chunk in HyperDex" + chunk.getPrimaryAttribute());
						System.exit(1);
					}
				}
	
				// PUT in S3
				for (Chunk chunk : chunks) {
					byte[] data = chunk.serialise(dr, Optional.of(secretKey));
					boolean success1 = s3.put(chunk, bucket, data);
					boolean success2 = minicrypt.put(chunk, cassandraTable, data, false); // Adding cassandra entries in chunks
					if (!success1 || !success2) {
						System.out.println("Failed to put chunk in S3 or in Cassandra" + chunk.getPrimaryAttribute());
						System.exit(1);
					}
				}
				
				// GET
				// 1. HyperDex GET
				Collection<ByteString> hdResults = hd.getTempRange(labels.get(0).low, labels.get(0).high, space, singleEntryChunks.size());

				// 2. S3 GET
				Collection<byte[]> s3Results = new ArrayList<byte[]>();
				long start = System.nanoTime();
				for (Chunk chunk : chunks) {
					s3Results.add(s3.get(chunk, bucket));
				}
				long s3GetAllElapsed = (System.nanoTime() - start)/1000000;
				
				// 2. Cassandra (MiniCrypt) GET
				Collection<byte[]> cassResults = minicrypt.getAll(cassandraTable);

				// 2. Cassandra GET range
				Collection<byte[]> cassSingleResults = cassSingle.getRange(labels.get(0).low, labels.get(0).high, cassandraTable);
				
				// Deserialise, decompress & decrypt if needed
				// 1. HyperDex Objects
				start = System.nanoTime();
				for (ByteString res : hdResults) {
					Chunk.deserialise(dr, res.getBytes(), Optional.of(secretKey));
				}
				long hdDecodeElapsed = (System.nanoTime() - start)/1000000; // maybe more detailed breakdown for decompression and decryption?
	
				// 2. S3 Objects: retrieve all of them
				start = System.nanoTime();
				for (byte[] res : s3Results) {
					Chunk.deserialise(dr, res, Optional.of(secretKey));
				}
				long s3DecodeElapsed = (System.nanoTime() - start)/1000000;
				
				// 2. Cassandra (MiniCrypt): deserialise all of them
				start = System.nanoTime();
				for (byte[] res : cassResults) {
					Chunk.deserialise(dr, res, Optional.of(secretKey));
				}
				long minicryptDecodeElapsed = (System.nanoTime() - start)/1000000;

				// 2. Cassandra (MiniCrypt): deserialise all of them
				start = System.nanoTime();
				for (byte[] res : cassSingleResults) {
					Chunk.deserialise(dr, res, Optional.of(secretKey));
				}
				long cassSingleDecodeElapsed = (System.nanoTime() - start)/1000000;
				
				// Search for the range of chunks (S3)
				start = System.nanoTime();
				searchRange(secretKey, labels, dr, s3Results);
				long avgS3Search = (System.nanoTime() - start)/1000000;

				// Search for the range of chunks (MiniCrypt)
				start = System.nanoTime();
				searchRange(secretKey, labels, dr, cassResults);
				long avgMiniCryptSearch = (System.nanoTime() - start)/1000000;
	
				printStats("HyperDex", dr, hd.getBenchmark().avgPut(), hd.getBenchmark().avgGet(), hdDecodeElapsed, 0);
				printStats("S3", dr, s3.getBenchmark().avgPut(), s3GetAllElapsed, s3DecodeElapsed, avgS3Search);
				printStats("Cassandra (MiniCrypt)", dr, minicrypt.getBenchmark().avgPut(), minicrypt.getBenchmark().avgGet(), minicryptDecodeElapsed, avgMiniCryptSearch);
				printStats("Cassandra", dr, cassSingle.getBenchmark().avgPut(), cassSingle.getBenchmark().avgGet(), cassSingleDecodeElapsed, 0);
				
				// Clean the state -> DEL items from the store, reset benchmarks
				// DEL
				for (Chunk chunk : singleEntryChunks) {
					hd.del(chunk, space, true);
				}
				for (Chunk chunk : chunks) {
					s3.del(chunk, bucket);
					minicrypt.del(chunk, cassandraTable);
				}
				hd.resetBenchmark();
				s3.resetBenchmark();
				minicrypt.resetBenchmark();
			}
			// Close Cassandra session
			minicrypt.close();
			
			/*
			 * Labelled Design Benchmark
			 */
			System.out.println("LABELLED DESIGN");
			// Chunk entries by their labels
			List<Chunk> labelledChunks = avaData.getLabelledChunks(maxChunkSize, labels.get(0));

			// Create space and bucket
			hd = new HyperDex(hyperdexIP, hyperdexPort);
			s3 = new S3(aws_access_key_id, aws_secret_access_key);
			minicrypt = new Cassandra(cassandraIP, cassandraPort);
	
			for (DataRepresentation dr : dataRepresentations) {
				// PUT
				for (Chunk chunk : labelledChunks) {
					byte[] data = chunk.serialise(dr, Optional.of(secretKey));
					boolean success1 = hd.put(chunk, labels.get(0).name, data, false);
					boolean success2 = s3.put(chunk, labels.get(0).name, data);
					boolean success3 = minicrypt.put(chunk, labels.get(0).name, data, false);
					if (!success1 || !success2 || !success3) {
						System.out.println("Failed to put chunk in HyperDex, S3 or Cassandra" + chunk.getPrimaryAttribute());
						System.exit(1);
					}
				}
				
				// GET
				Collection<ByteString> hdResults = new ArrayList<>();
				Collection<byte[]> s3Results = new ArrayList<>();
				Collection<byte[]> cassResults;
				
				long start = System.nanoTime();
				for (Chunk chunk : labelledChunks) {
					hdResults.add(hd.get(chunk, labels.get(0).name));
				}
				long hdGetAllElapsed = (System.nanoTime() - start)/1000000;
				
				start = System.nanoTime();
				for (Chunk chunk: labelledChunks) {
					s3Results.add(s3.get(chunk, labels.get(0).name));
				}
				long s3GetAllElapsed = (System.nanoTime() - start)/1000000;
				
				cassResults = minicrypt.getAll(labels.get(0).name);
				
				// Deserialise, decrypt and decompress
				start = System.nanoTime();
				for (ByteString res: hdResults) {
					Chunk.deserialise(dr, res.getBytes(), Optional.of(secretKey));
				}
				long hdDecodeElapsed = (System.nanoTime() - start)/1000000; // maybe more detailed breakdown for decompression and decryption?
	
				start = System.nanoTime();
				for (byte[] res: s3Results) {
					Chunk.deserialise(dr, res, Optional.of(secretKey));
				}
				long s3DecodeElapsed = (System.nanoTime() - start)/1000000;
	
				start = System.nanoTime();
				for (byte[] res: cassResults) {
					Chunk.deserialise(dr, res, Optional.of(secretKey));
				}
				long cassDecodeElapsed = (System.nanoTime() - start)/1000000;
				
				printStats("HyperDex", dr, hd.getBenchmark().avgPut(), hdGetAllElapsed, hdDecodeElapsed, 0);
				printStats("S3", dr, s3.getBenchmark().avgPut(), s3GetAllElapsed, s3DecodeElapsed, 0); 
				printStats("Cassandra", dr, minicrypt.getBenchmark().avgPut(), minicrypt.getBenchmark().avgGet(), cassDecodeElapsed, 0);
	
				// Clean the state -> DEL items from the store, reset benchmarks
				// DEL
				for (Chunk chunk : labelledChunks) {
					hd.del(chunk, labels.get(0).name, false);
					s3.del(chunk, labels.get(0).name);
					minicrypt.del(chunk, labels.get(0).name);
				}
				hd.resetBenchmark();
				s3.resetBenchmark();
				minicrypt.resetBenchmark();
			}
			// Close Cassandra session
			minicrypt.close();
		}
	}

	private static Collection<Chunk> searchRange(SecretKey secretKey, List<Label> labels, DataRepresentation dr, Collection<byte[]> results) {
		Collection<Chunk> chunksInRange = new ArrayList<>();
		for (byte[] res: results) {
			Chunk c = Chunk.deserialise(dr, res, Optional.of(secretKey));
			if (c.secondAttribute > labels.get(0).low && c.secondAttribute < labels.get(0).high) {
				chunksInRange.add(c);
			}
		}
		
		return chunksInRange;
	}

	private static void printStats(String datastore, DataRepresentation dr, double avgPut, double avgGet, double avgDecode, double avgSearch) {
		System.out.format("%s\t%s\t%s\t%s\t%s\t%s\n", datastore, dr, avgPut, avgGet, avgDecode, avgSearch);
	}
}
