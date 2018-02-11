package ch.michel.test;

import ch.lubu.AvaData;
import treedb.client.TreeDB;
import treedb.client.security.CryptoKeyPair;
import ch.lubu.Chunk;
import ch.michel.DataRepresentation;
import ch.michel.S3;
import ch.michel.Utility;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
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

    public static void main(String[] args) throws IOException {
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
        for (int i = 0; i < 19; i++) { // copy to get to 600 chunks amounts for better results
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
        String streamID = trDB.createStream(2, "{ 'sum': true }", keys.publicKey, "S3");

        // Populate S3 and TreeDB
        for (Chunk c : chunks) {
            byte[] data = c.serialise(DR, Optional.of(secretKey));
            s3.put(c, BASELINE_S3_BUCKET, data);

            BigInteger plainSum = c.getSum();
            BigInteger encryptedSum = keys.publicKey.raw_encrypt_without_obfuscation(plainSum);
            data = c.serialise(DR, Optional.of(secretKey));
            String metadata = String.format("{ 'from': %s, 'to': %s, 'sum': %s }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), encryptedSum);
            trDB.insert(streamID, c.getPrimaryAttribute(), data, metadata); // append chunk to the index
        }

        System.out.format("%s\t%s\t%s\t%s\n", "Data Store", "Retrieval & Decryption", "Sum computation", "Total");   

        for (int i=0; i < experimentReps; i++) {
            System.out.println("Experiment number #" + i);

            /**
             * S3
             */
            for (int j = 0; j < chunks.size(); j++) {
                // GET from S3 & calculate the sum of all chunks
                long start = System.nanoTime();
                List<Chunk> retrievedChunks = new ArrayList<>();
                for (Chunk chunk : chunks.subList(0, j+1)) {
                    byte[] res = s3.get(chunk, BASELINE_S3_BUCKET);
                    retrievedChunks.add(Chunk.deserialise(DR, res, Optional.of(secretKey)));
                }
                long retrievalTime = (System.nanoTime() - start)/1000000;

                // Calculate sum locally 
                start = System.nanoTime();
                BigInteger s3Sum = BigInteger.ZERO;
                for (Chunk chunk : retrievedChunks) {
                    s3Sum = s3Sum.add(chunk.getSum());
                }
                long sumComputationTime = (System.nanoTime() - start)/1000000;
                
                printStats("S3", retrievalTime, sumComputationTime);
            }

            /**
             * TreeDB + S3
             */
            for (int j = 0; j < chunks.size(); j++) {
                // GET from TreeDB
                long start = System.nanoTime();
                String stats = trDB.getStatistics(streamID, chunks.get(0).getFirstEntry().getTimestamp(), chunks.get(j).getLastEntry().getTimestamp());

                JsonParser parser = new JsonParser();
                JsonObject jObj = parser.parse(stats).getAsJsonObject();
                BigInteger treeDBEncryptedSum = jObj.get("sum").getAsBigInteger();
                BigInteger decryptedSum = keys.privateKey.raw_decrypt(treeDBEncryptedSum);
                long treeDbRetrievalTime = (System.nanoTime() - start)/1000000;
                printStats("TreeDB+S3", treeDbRetrievalTime, 0);
            }
        }

        // Clean state of the experiment
        cleanState(chunks, s3, BASELINE_S3_BUCKET);
        cleanState(chunks, s3, streamID);
        trDB.closeConnection();
    }

    private static void cleanState(List<Chunk> chunks, S3 s3, String bucketName) {
        for (Chunk c : chunks) {
            s3.del(c, bucketName);
        }
    }

	private static void printStats(String datastore, double avgGet, double avgDecode) {
		System.out.format("%s\t%s\t%s\t%s\n", datastore, avgGet, avgDecode, avgGet+avgDecode);
	}
}
