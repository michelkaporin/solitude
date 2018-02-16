package ch.michel.test;

import ch.lubu.AvaData;
import treedb.client.TreeDB;
import treedb.client.security.CryptoKeyPair;
import treedb.client.security.ECElGamalWrapper;
import treedb.client.security.OREWrapper;
import treedb.client.security.OPEWrapper;
import ch.lubu.Chunk;
import ch.michel.DataRepresentation;
import ch.michel.S3;
import ch.michel.Utility;
import ch.michel.test.Helpers.TreeDBBenchmarkStats;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import javax.crypto.SecretKey;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * Running locally:
 * java -classpath ".:lib/aws-sdk/lib/aws-java-sdk-1.11.255.jar:lib/aws-sdk/third-party/lib/*:/Users/michel/Git/treedb/client/target/treedb.client-0.0.1-SNAPSHOT-jar-with-dependencies.jar" --add-modules=java.xml.bind,java.activation ch/michel/test/TreeDBBenchmark 1000 1 AKIAI6DALQXSPRIEAPWQ zerBOTixXhgqvJuij00evoJOOHYDAuYR9cYTZXYy 127.0.0.1 8001 >> LOG.log
 */
public class TreeDBBenchmark {
    private static String BASELINE_S3_BUCKET = "treedb-baseline";
    private static DataRepresentation DR = DataRepresentation.CHUNKED_COMPRESSED_ENCRYPTED;

    public static void main(String[] args) throws Exception {
        int maxChunkSize = Integer.valueOf(args[0]);
		int experimentReps = Integer.valueOf(args[1]);
		String aws_access_key_id = args[2];
        String aws_secret_access_key = args[3];
        String treedbIP = args[4];
		int treedbPort = Integer.valueOf(args[5]);
        SecretKey secretKey = Utility.generateSecretKey();

        // Extract and duplicate data to have enough chunks
        AvaData avaData = new AvaData();
        List<Chunk> chunks = avaData.getChunks(maxChunkSize, false, true); // 30 chunks
        List<Chunk> lastChunks = chunks;
        for (int i = 0; i < 34; i++) { // copy over to get to 1030 chunks for a better statistical view
            List<Chunk> newChunks = new ArrayList<>();
            lastChunks.forEach(c -> newChunks.add(c.copy(86400*30))); // add 30 days on top of the current data
            chunks.addAll(newChunks); 
            lastChunks = newChunks;
        }

        S3 s3 = new S3(aws_access_key_id, aws_secret_access_key);
        cleanState(chunks, s3, BASELINE_S3_BUCKET);
        TreeDB trDB = new TreeDB(treedbIP, treedbPort);
        trDB.openConnection();
        CryptoKeyPair keys = CryptoKeyPair.generateKeyPair();
        ECElGamalWrapper ecelgamal = new ECElGamalWrapper();
        OREWrapper ore = new OREWrapper();
        OPEWrapper ope = new OPEWrapper();

        String paillierStreamID = trDB.createStream(2, "{ 'sum': true }", keys.publicKey, "S3");
        String ecelGamalStreamID = trDB.createStream(2, "{ 'sum': true, 'algorithms': { 'sum': 'ecelgamal' } }", null, "S3");
        String opeStreamID = trDB.createStream(2, "{ 'max': true }", null, "S3");
        String oreStreamID = trDB.createStream(2, "{ 'max': true, 'algorithms': { 'max': 'ore' } }", null, "S3");

        // Populate S3 and TreeDB
        for (Chunk c : chunks) {
            byte[] data = c.serialise(DR, Optional.of(secretKey));
            s3.put(c, BASELINE_S3_BUCKET, data);

            BigInteger plainSum = c.getSum();
            data = c.serialise(DR, Optional.of(secretKey));

            String paillierSumMetadata = String.format("{ 'from': %s, 'to': %s, 'sum': %s }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), keys.publicKey.raw_encrypt_without_obfuscation(plainSum));
            trDB.insert(paillierStreamID, c.getPrimaryAttribute(), data, paillierSumMetadata); // append chunk to the index

            String ecelGamalSumMetadata = String.format("{ 'from': %s, 'to': %s, 'sum': '%s' }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), ecelgamal.encryptAndEncode(plainSum));
            trDB.insert(ecelGamalStreamID, c.getPrimaryAttribute(), data, ecelGamalSumMetadata);
        
            String opeMaxMetadata = String.format("{ 'from': %s, 'to': %s, 'max': %s }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), ope.encrypt(plainSum));
            trDB.insert(opeStreamID, c.getPrimaryAttribute(), data, opeMaxMetadata);

            String oreMxMetadata = String.format("{ 'from': %s, 'to': %s, 'max': '%s' }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), ore.encryptAndEncode(plainSum));
            trDB.insert(oreStreamID, c.getPrimaryAttribute(), data, oreMxMetadata);
        }

        System.out.format("%s\t%s\t%s\t%s\n", "Data Store", "Retrieval", "Decode & Decryption", "Sum computation", "Total");   

        List<TreeDBBenchmarkStats> s3SumStats = new ArrayList<>();
        List<TreeDBBenchmarkStats> s3MaxStats = new ArrayList<>();
        List<TreeDBBenchmarkStats> paillierStats = new ArrayList<>();
        List<TreeDBBenchmarkStats> ecelGamalStats = new ArrayList<>();
        List<TreeDBBenchmarkStats> opeStats = new ArrayList<>();
        List<TreeDBBenchmarkStats> oreStats = new ArrayList<>();
        
        for (int i=0; i < experimentReps; i++) {
            System.out.println("Experiment number #" + i);

            /**
             * S3
             */
            for (int j = 0; j < chunks.size(); j++) {
                // GET from S3 & calculate the sum of all chunks
                long start = System.nanoTime();
                List<byte[]> retrievedChunksBytes = new ArrayList<>();
                for (Chunk chunk : chunks.subList(0, j+1)) { // Retrieve from range of 0 to j (j+1 exclusive)
                    retrievedChunksBytes.add(s3.get(chunk, BASELINE_S3_BUCKET));
                }
                long s3RetrievalTime = (System.nanoTime() - start)/1000000;

                start = System.nanoTime();
                List<Chunk> retrievedChunks = new ArrayList<>();
                for (byte[] bytes : retrievedChunksBytes) {
                    retrievedChunks.add(Chunk.deserialise(DR, bytes, Optional.of(secretKey)));
                }
                long s3DecodeDecryptTime = (System.nanoTime() - start)/1000000;

                // Calculate sum locally 
                start = System.nanoTime();
                BigInteger s3Sum = BigInteger.ZERO;
                for (Chunk chunk : retrievedChunks) {
                    s3Sum = s3Sum.add(chunk.getSum());
                }
                long s3SumComputationTime = (System.nanoTime() - start)/1000000;

                // Calculate max locally 
                start = System.nanoTime();
                BigInteger max = retrievedChunks.get(0).getMax();
                for (Chunk chunk : retrievedChunks) {
                    max = max.max(chunk.getMax());
                }
                long s3MaxComputationTime = (System.nanoTime() - start)/1000000;
                
                // Populate stats 
                s3SumStats.add(new TreeDBBenchmarkStats("S3", s3RetrievalTime, s3DecodeDecryptTime, s3SumComputationTime));
                s3MaxStats.add(new TreeDBBenchmarkStats("S3", s3RetrievalTime, s3DecodeDecryptTime, s3MaxComputationTime));
            }

            /**
             * TreeDB + S3
             */
            for (int j = 0; j < chunks.size(); j++) {
                // GET Paillier sum
                long start = System.nanoTime();
                String stats = trDB.getStatistics(paillierStreamID, chunks.get(0).getFirstEntry().getTimestamp(), chunks.get(j).getLastEntry().getTimestamp());
                float treeDbRetrievalTime = timestamp(start);
                
                start = System.nanoTime();
                JsonParser parser = new JsonParser();
                JsonObject jObj = parser.parse(stats).getAsJsonObject();
                BigInteger decryptedSum = keys.privateKey.raw_decrypt(jObj.get("sum").getAsBigInteger());
                float treeDbDecodeTime = timestamp(start);

                paillierStats.add(new TreeDBBenchmarkStats("TreeDB+S3", treeDbRetrievalTime, treeDbDecodeTime, 0));

                // GET EC El Gamal Sum
                start = System.nanoTime();
                stats = trDB.getStatistics(ecelGamalStreamID, chunks.get(0).getFirstEntry().getTimestamp(), chunks.get(j).getLastEntry().getTimestamp());
                treeDbRetrievalTime = timestamp(start);
                
                start = System.nanoTime();
                parser = new JsonParser();
                jObj = parser.parse(stats).getAsJsonObject();
                BigInteger decryptedSum2 = ecelgamal.decodeAndDecrypt(jObj.get("sum").getAsString());
                treeDbDecodeTime = timestamp(start);

                ecelGamalStats.add(new TreeDBBenchmarkStats("TreeDB+S3", treeDbRetrievalTime, treeDbDecodeTime, 0));
                
                // GET OPE Max
                start = System.nanoTime();
                stats = trDB.getStatistics(opeStreamID, chunks.get(0).getFirstEntry().getTimestamp(), chunks.get(j).getLastEntry().getTimestamp());
                treeDbRetrievalTime = timestamp(start);
                
                start = System.nanoTime();
                parser = new JsonParser();
                jObj = parser.parse(stats).getAsJsonObject();
                BigInteger decryptedMax = ope.decrypt(jObj.get("max").getAsBigInteger());
                treeDbDecodeTime = timestamp(start);

                opeStats.add(new TreeDBBenchmarkStats("TreeDB+S3", treeDbRetrievalTime, treeDbDecodeTime, 0));

                // GET ORE Max
                start = System.nanoTime();
                stats = trDB.getStatistics(oreStreamID, chunks.get(0).getFirstEntry().getTimestamp(), chunks.get(j).getLastEntry().getTimestamp());
                treeDbRetrievalTime = timestamp(start);
                
                start = System.nanoTime();
                parser = new JsonParser();
                jObj = parser.parse(stats).getAsJsonObject();
                BigInteger decryptedMax2 = ore.decodeAndDecrypt(jObj.get("max").getAsString());
                treeDbDecodeTime = timestamp(start);

                oreStats.add(new TreeDBBenchmarkStats("TreeDB+S3", treeDbRetrievalTime, treeDbDecodeTime, 0));
            }
        }

        // Print Resulting stats
        printStats("*** S3 SUM ***", s3SumStats);
        printStats("*** S3 MAX ***", s3MaxStats);
        printStats("*** TreeDB Paillier SUM ***", paillierStats);
        printStats("*** TreeDB EC El Gamal SUM ***", ecelGamalStats);
        printStats("*** TreeDB Order-Preserving Encryption MAX ***", opeStats);
        printStats("*** TreeDB Order-Revealing Encryption MAX ***", oreStats);

        // Clean state of the experiment
        cleanState(chunks, s3, BASELINE_S3_BUCKET);
        cleanState(chunks, s3, paillierStreamID);
        cleanState(chunks, s3, ecelGamalStreamID);
        cleanState(chunks, s3, opeStreamID);
        cleanState(chunks, s3, oreStreamID);
        trDB.closeConnection();
    }

    private static void cleanState(List<Chunk> chunks, S3 s3, String bucketName) {
        for (Chunk c : chunks) {
            s3.del(c, bucketName);
        }
    }

	private static void printStats(String header, List<TreeDBBenchmarkStats> stats) {
        System.out.println(header);
        for (TreeDBBenchmarkStats s : stats) {
            float overall = s.retrievalTime+s.decodeDecryptTime+s.computationTime;
            System.out.format("%s\t%s\t%s\t%s\t%s\n", s.design, String.format(Locale.ROOT, "%.2f", s.retrievalTime), String.format(Locale.ROOT, "%.2f", s.decodeDecryptTime), String.format(Locale.ROOT, "%.2f", s.computationTime), String.format(Locale.ROOT, "%.2f", overall));
        }
    }
    
    private static float timestamp(long start) {
        return ((float) (System.nanoTime() - start))/1000000;
    }
}
