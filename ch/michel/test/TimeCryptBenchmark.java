package ch.michel.test;

import ch.lubu.AvaData;
import timecrypt.client.TimeCrypt;
import timecrypt.client.security.ECElGamalWrapper;
import timecrypt.client.security.OREWrapper;
import timecrypt.client.security.PaillierWrapper;
import timecrypt.client.security.OPEWrapper;
import ch.lubu.Chunk;
import ch.michel.DataRepresentation;
import ch.michel.S3;
import ch.michel.Utility;
import ch.michel.test.helpers.TimeCryptBenchmarkStats;
import ch.michel.test.timecrypt.BaselineMetadata;
import ch.michel.test.timecrypt.TimeCryptBaselineClient;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.crypto.SecretKey;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 * Running:
 * java -classpath ".:lib/aws-sdk/lib/aws-java-sdk-1.11.255.jar:lib/aws-sdk/third-party/lib/*:lib/timecrypt.client-0.0.1-SNAPSHOT-jar-with-dependencies.jar" --add-modules=java.xml.bind,java.activation ch/michel/test/TimeCryptBenchmark 1000 1 AKIAI6DALQXSPRIEAPWQ zerBOTixXhgqvJuij00evoJOOHYDAuYR9cYTZXYy 127.0.0.1 8001 >> LOG.log
 * 
 * Compiling:
 * javac -classpath ".:/usr/local/share/java/org.hyperdex.client-1.8.1.jar:lib/aws-sdk/lib/aws-java-sdk-1.11.255.jar:lib/aws-sdk/third-party/lib/*:lib/cassandra/cassandra-driver-core-3.3.2.jar:lib/cassandra/lib/*:lib/timecrypt.client-0.0.1-SNAPSHOT-jar-with-dependencies.jar" $(find . -name '*.java')
 */
public class TimeCryptBenchmark {
    private static String BASELINE_S3_BUCKET = "timecrypt-baseline";
    private static DataRepresentation DR = DataRepresentation.CHUNKED_COMPRESSED_ENCRYPTED;
    private static int[] kChildren;
    private static String streamStorage = null;

    public static void main(String[] args) throws Exception {
        int maxChunkSize = Integer.valueOf(args[0]);
		int experimentReps = Integer.valueOf(args[1]);
        String aws_access_key_id = args[2];
        String aws_secret_access_key = args[3];
        String timecryptIP = args[4];
        int timecryptPort = Integer.valueOf(args[5]);

        boolean varyK = Boolean.valueOf(args[6]);

        boolean s3Baseline = false;
        String trdbBaselineIP = null; 
        int trdbBaselinePort = 0;
        try {
            trdbBaselineIP = args[7];
            trdbBaselinePort = Integer.valueOf(args[8]);
        } catch (Exception e) {
            s3Baseline = true;
        }

        try {
            if (s3Baseline) streamStorage = args[7];
            else streamStorage = args[9];
        } catch (Exception e) { }

        if (varyK) {
            kChildren = new int[] { 2, 4, 16, 32, 64 };
        } else {
            kChildren = new int[] { 2 }; // Have K = 2 as default for kChildren
        }

        SecretKey secretKey = Utility.generateSecretKey();

        // Extract and duplicate data to have enough chunks
        AvaData avaData = new AvaData();
        List<Chunk> chunks = avaData.getChunks(maxChunkSize, false, true, true); // 30 chunks
        List<Chunk> lastChunks = chunks;
        for (int i = 0; i < 0; i++) { // copy 33 times over to get to 1030 chunks for a better statistical view
            List<Chunk> newChunks = new ArrayList<>();
            lastChunks.forEach(c -> newChunks.add(c.copy(86400*30))); // add 30 days on top of the current data
            chunks.addAll(newChunks); 
            lastChunks = newChunks;
        }

        S3 s3 = new S3(aws_access_key_id, aws_secret_access_key);
        TimeCryptBaselineClient trdbBaseline = null;
        if (s3Baseline) {
            cleanState(chunks, s3, BASELINE_S3_BUCKET, false);        
        } else {
            // Create connection to TimeCrypt Baseline server
            trdbBaseline = new TimeCryptBaselineClient(trdbBaselineIP, trdbBaselinePort);
            trdbBaseline.openConnection();
        }

        TimeCrypt timecrypt = new TimeCrypt(timecryptIP, timecryptPort);
        timecrypt.openConnection();
        PaillierWrapper paillier = new PaillierWrapper();
        ECElGamalWrapper ecelgamal = new ECElGamalWrapper();
        OREWrapper ore = new OREWrapper();
        OPEWrapper ope = new OPEWrapper();

        List<String> paillierStreamsID = new ArrayList<String>();
        List<String> ecelGamalStreamsID = new ArrayList<String>();
        List<String> opeStreamsID = new ArrayList<String>();
        List<String> oreStreamsID = new ArrayList<String>();

        for (int k : kChildren) {
            paillierStreamsID.add(timecrypt.createStream(k, "{ 'sum': true }", paillier.getPublicKey(), streamStorage));
            ecelGamalStreamsID.add(timecrypt.createStream(k, "{ 'sum': true, 'algorithms': { 'sum': 'ecelgamal' } }", null, streamStorage));
            opeStreamsID.add(timecrypt.createStream(k, "{ 'max': true }", null, streamStorage));
            oreStreamsID.add(timecrypt.createStream(k, "{ 'max': true, 'algorithms': { 'max': 'ore' } }", null, streamStorage));
        }

        // Populate S3/TimeCrypt-Baseline and TimeCrypt
        for (Chunk c : chunks) {
            byte[] data = c.serialise(DR, Optional.of(secretKey));
            BigInteger plainSum = c.getSum();
            
            if (s3Baseline) { 
                s3.put(c, BASELINE_S3_BUCKET, data); 
            } else {
                String metadata = new BaselineMetadata(String.format("{ 'sum': %s, 'max': %s }", plainSum.toString(), plainSum.toString())).getEncodedEncryptedData(secretKey.getEncoded());

                trdbBaseline.insert(c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), metadata);
            }

            for (int i = 0; i < kChildren.length; i++) {
                String paillierSumMetadata = String.format("{ 'from': %s, 'to': %s, 'sum': %s }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), paillier.encrypt(plainSum));
                timecrypt.insert(paillierStreamsID.get(i), c.getPrimaryAttribute(), data, paillierSumMetadata); // append chunk to the index

                String ecelGamalSumMetadata = String.format("{ 'from': %s, 'to': %s, 'sum': '%s' }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), ecelgamal.encryptAndEncode(plainSum));
                timecrypt.insert(ecelGamalStreamsID.get(i), c.getPrimaryAttribute(), data, ecelGamalSumMetadata);
            
                String opeMaxMetadata = String.format("{ 'from': %s, 'to': %s, 'max': %s }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), ope.encrypt(plainSum));
                timecrypt.insert(opeStreamsID.get(i), c.getPrimaryAttribute(), data, opeMaxMetadata);

                String oreMxMetadata = String.format("{ 'from': %s, 'to': %s, 'max': '%s' }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), ore.encryptAndEncode(plainSum));
                timecrypt.insert(oreStreamsID.get(i), c.getPrimaryAttribute(), data, oreMxMetadata);
            }
        }

        List<TimeCryptBenchmarkStats> baselineSumStats = new ArrayList<TimeCryptBenchmarkStats>();
        List<TimeCryptBenchmarkStats> baselineMaxStats = new ArrayList<TimeCryptBenchmarkStats>();
        Map<Integer, List<TimeCryptBenchmarkStats>> paillierStats = new HashMap<Integer, List<TimeCryptBenchmarkStats>>();
        Map<Integer, List<TimeCryptBenchmarkStats>> ecelGamalStats = new HashMap<Integer, List<TimeCryptBenchmarkStats>>();
        Map<Integer, List<TimeCryptBenchmarkStats>> opeStats = new HashMap<Integer, List<TimeCryptBenchmarkStats>>();
        Map<Integer, List<TimeCryptBenchmarkStats>> oreStats = new HashMap<Integer, List<TimeCryptBenchmarkStats>>();
        initStatsLists(paillierStats, ecelGamalStats, opeStats, oreStats);
       
        System.out.format("%s\t%s\t%s\t%s\t%s\n", "Data Store", "Retrieval", "Decode & Decryption", "Sum computation", "Total");   
        for (int i=0; i < experimentReps; i++) {
            System.out.println("Experiment number #" + i);

            /**
             * S3
             */
            if (s3Baseline) {
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
                    baselineSumStats.add(new TimeCryptBenchmarkStats("S3", s3RetrievalTime, s3DecodeDecryptTime, s3SumComputationTime));
                    baselineMaxStats.add(new TimeCryptBenchmarkStats("S3", s3RetrievalTime, s3DecodeDecryptTime, s3MaxComputationTime));
                }
            } else {
                // TimeCrypt Baseline
                for (int j = 0; j < chunks.size(); j++) {
                    long start = System.nanoTime();
                    List<byte[]> metadataList = trdbBaseline.getRange(chunks.get(0).getFirstEntry().getTimestamp(), chunks.get(j).getLastEntry().getTimestamp());
                    float retrievalTime = timestamp(start);

                    List<String> metadataStrings = new ArrayList<>();
                    start = System.nanoTime();
                    for (byte[] md : metadataList) {
                        String metadata = BaselineMetadata.decryptMetadata(md, secretKey.getEncoded());
                        metadataStrings.add(metadata);
                    }
                    float decodeDecryptTime = timestamp(start);

                    // Compute sum
                    start = System.nanoTime();
                    JsonParser parser = new JsonParser();
                    JsonObject jObj = null;
                    BigInteger sum = BigInteger.ZERO;
                    for (String md : metadataStrings) {
                        jObj = parser.parse(md).getAsJsonObject();
                        sum = sum.add(jObj.get("sum").getAsBigInteger());
                    }
                    float sumComputationTime = timestamp(start);

                    // Compute max
                    start = System.nanoTime();
                    parser = new JsonParser();
                    BigInteger max = BigInteger.ZERO;
                    for (String md : metadataStrings) {
                        jObj = parser.parse(md).getAsJsonObject();
                        max = max.max(jObj.get("max").getAsBigInteger());
                    }
                    float maxComputationTime = timestamp(start);

                    baselineSumStats.add(new TimeCryptBenchmarkStats("TimeCrypt Baseline", retrievalTime, decodeDecryptTime, sumComputationTime));
                    baselineMaxStats.add(new TimeCryptBenchmarkStats("TimeCrypt Baseline", retrievalTime, decodeDecryptTime, maxComputationTime));
                }
            }

            /**
             * TimeCrypt + S3
             */
            for (int j = 0; j < chunks.size(); j++) {
                for (int k = 0; k < kChildren.length; k++) {
                    // GET Paillier sum
                    long start = System.nanoTime();
                    String stats = timecrypt.getStatistics(paillierStreamsID.get(k), chunks.get(0).getFirstEntry().getTimestamp(), chunks.get(j).getLastEntry().getTimestamp());
                    float treeDbRetrievalTime = timestamp(start);
                    
                    start = System.nanoTime();
                    JsonParser parser = new JsonParser();
                    JsonObject jObj = parser.parse(stats).getAsJsonObject();
                    BigInteger decryptedSum = paillier.decrypt(jObj.get("sum").getAsBigInteger());
                    float treeDbDecodeTime = timestamp(start);

                    paillierStats.get(k).add(new TimeCryptBenchmarkStats("TimeCrypt+S3", treeDbRetrievalTime, treeDbDecodeTime, 0));

                    // GET EC El Gamal Sum
                    start = System.nanoTime();
                    stats = timecrypt.getStatistics(ecelGamalStreamsID.get(k), chunks.get(0).getFirstEntry().getTimestamp(), chunks.get(j).getLastEntry().getTimestamp());
                    treeDbRetrievalTime = timestamp(start);
                    
                    start = System.nanoTime();
                    parser = new JsonParser();
                    jObj = parser.parse(stats).getAsJsonObject();
                    BigInteger decryptedSum2 = ecelgamal.decodeAndDecrypt(jObj.get("sum").getAsString());
                    treeDbDecodeTime = timestamp(start);

                    ecelGamalStats.get(k).add(new TimeCryptBenchmarkStats("TimeCrypt+S3", treeDbRetrievalTime, treeDbDecodeTime, 0));
                    
                    // GET OPE Max
                    start = System.nanoTime();
                    stats = timecrypt.getStatistics(opeStreamsID.get(k), chunks.get(0).getFirstEntry().getTimestamp(), chunks.get(j).getLastEntry().getTimestamp());
                    treeDbRetrievalTime = timestamp(start);
                    
                    start = System.nanoTime();
                    parser = new JsonParser();
                    jObj = parser.parse(stats).getAsJsonObject();
                    BigInteger decryptedMax = ope.decrypt(jObj.get("max").getAsBigInteger());
                    treeDbDecodeTime = timestamp(start);

                    opeStats.get(k).add(new TimeCryptBenchmarkStats("TimeCrypt+S3", treeDbRetrievalTime, treeDbDecodeTime, 0));

                    // GET ORE Max
                    start = System.nanoTime();
                    stats = timecrypt.getStatistics(oreStreamsID.get(k), chunks.get(0).getFirstEntry().getTimestamp(), chunks.get(j).getLastEntry().getTimestamp());
                    treeDbRetrievalTime = timestamp(start);
                    
                    start = System.nanoTime();
                    parser = new JsonParser();
                    jObj = parser.parse(stats).getAsJsonObject();
                    BigInteger decryptedMax2 = ore.decodeAndDecrypt(jObj.get("max").getAsString());
                    treeDbDecodeTime = timestamp(start);

                    oreStats.get(k).add(new TimeCryptBenchmarkStats("TimeCrypt+S3", treeDbRetrievalTime, treeDbDecodeTime, 0));
                }
            }

            // Print resulting stats and clear them from memory
            printStats("*** Baseline SUM ***", baselineSumStats);
            printStats("*** Baseline MAX ***", baselineMaxStats);
            baselineSumStats.clear();
            baselineMaxStats.clear();

            for (int j = 0; j < kChildren.length; j++) {
                System.out.println("\nK = " + kChildren[j] + "\n");
                printStats("*** TimeCrypt Paillier SUM ***", paillierStats.get(j));
                printStats("*** TimeCrypt EC El Gamal SUM ***", ecelGamalStats.get(j));
                printStats("*** TimeCrypt Order-Preserving Encryption MAX ***", opeStats.get(j));
                printStats("*** TimeCrypt Order-Revealing Encryption MAX ***", oreStats.get(j));
                paillierStats.get(j).clear();
                ecelGamalStats.get(j).clear();
                opeStats.get(j).clear();
                oreStats.get(j).clear();
            }
        }

        // Clean state of the experiment
        if (s3Baseline) cleanState(chunks, s3, BASELINE_S3_BUCKET, false);
        else trdbBaseline.clean();
        for (int k=0; k < kChildren.length; k++) {
            cleanState(chunks, s3, paillierStreamsID.get(k), true);
            cleanState(chunks, s3, ecelGamalStreamsID.get(k), true);
            cleanState(chunks, s3, opeStreamsID.get(k), true);
            cleanState(chunks, s3, oreStreamsID.get(k), true);
        }
        timecrypt.closeConnection();
    }

    private static void initStatsLists(Map<Integer, List<TimeCryptBenchmarkStats>>... stats) {
        for (Map<Integer, List<TimeCryptBenchmarkStats>> s : stats) {
            for (int k = 0; k < kChildren.length; k++) {
                s.put(k, new ArrayList<TimeCryptBenchmarkStats>());
            }
        }
	}

	private static void cleanState(List<Chunk> chunks, S3 s3, String bucketName, boolean deleteBucket) {
        for (Chunk c : chunks) {
            s3.del(c, bucketName);
        }
        if (deleteBucket) s3.deleteBucket(bucketName);        
    }

	private static void printStats(String header, List<TimeCryptBenchmarkStats> stats) {
        System.out.println(header);
        for (TimeCryptBenchmarkStats s : stats) {
            float overall = s.retrievalTime+s.decodeDecryptTime+s.computationTime;
            System.out.format("%s\t%s\t%s\t%s\t%s\n", s.design, String.format(Locale.ROOT, "%.2f", s.retrievalTime), String.format(Locale.ROOT, "%.2f", s.decodeDecryptTime), String.format(Locale.ROOT, "%.2f", s.computationTime), String.format(Locale.ROOT, "%.2f", overall));
        }
        System.out.println();
    }
    
    private static float timestamp(long start) {
        return ((float) (System.nanoTime() - start))/1000000;
    }
}
