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
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Optional;
import javax.crypto.SecretKey;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.n1analytics.paillier.EncryptedNumber;
import com.n1analytics.paillier.PaillierContext;

public class TreeDBBenchmark {

    public static void main(String[] args) throws IOException {
        int maxChunkSize = Integer.valueOf(args[0]);
		int experimentReps = Integer.valueOf(args[1]);
		String aws_access_key_id = args[2];
        String aws_secret_access_key = args[3];
        String treedbIP = args[4];
		int treedbPort = Integer.valueOf(args[5]);
        SecretKey secretKey = Utility.generateSecretKey();

        AvaData avaData = new AvaData();
        List<Chunk> chunks = avaData.getChunks(maxChunkSize, false, true);
		S3 s3 = new S3(aws_access_key_id, aws_secret_access_key);
        String baselineBucket = "treedb-baseline";
        DataRepresentation dr = DataRepresentation.CHUNKED_COMPRESSED_ENCRYPTED;

        System.out.format("%s\t%s\t%s\t%s\n", "Data Store", "Retrieval & Decryption", "Sum computation", "Total");        
        for (int i=0; i < experimentReps; i++) {
            // Populate S3
            for (Chunk chunk : chunks) {
                byte[] data = chunk.serialise(dr, Optional.of(secretKey));
                s3.put(chunk, baselineBucket, data);
            }
            
            // Populate index with S3 as a storage layer
            TreeDB trDB = new TreeDB(treedbIP, treedbPort);
            trDB.openConnection();
            CryptoKeyPair keys = CryptoKeyPair.generateKeyPair();
            String streamID = trDB.createStream(2, "{ 'sum': true }", keys.publicKey);
            BigInteger actualSum = BigInteger.ZERO;

            // TESTT
            PaillierContext pContext = keys.publicKey.createSignedContext();
            EncryptedNumber encSum = null;

            for (Chunk chunk : chunks) {
                BigInteger plainSum = chunk.getSum();
                actualSum = actualSum.add(plainSum);

                BigInteger encryptedSum = keys.publicKey.raw_encrypt_without_obfuscation(plainSum);
                encSum = encSum == null ? new EncryptedNumber(pContext, encryptedSum, 2048) : encSum.add(new EncryptedNumber(pContext, encryptedSum, 2048));
                
                byte[] data = chunk.serialise(dr, Optional.of(secretKey));
                String metadata = String.format("{ 'from': %s, 'to': %s, 'sum': %s }", chunk.getFirstEntry().getTimestamp(), chunk.getLastEntry().getTimestamp(), encryptedSum); //
                trDB.insert(streamID, chunk.getPrimaryAttribute(), data, metadata);
            }

            /**
             * Baseline S3
             */
            // GET from S3 & calculate the sum of all chunks
            long start = System.nanoTime();
            List<Chunk> retrievedChunks = new ArrayList<>();
            for (Chunk chunk : chunks) {
                byte[] res = s3.get(chunk, baselineBucket);
                retrievedChunks.add(Chunk.deserialise(dr, res, Optional.of(secretKey)));
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

            /**
             * TreeDB + S3
             */
            // GET from TreeDB
            start = System.nanoTime();
            String stats = trDB.getStatistics(streamID, new GregorianCalendar(2000, 1, 1).getTimeInMillis()/1000, new Date().getTime()/1000); // convert to seconds to match Ava timestamps 

            JsonParser parser = new JsonParser();
            JsonObject jObj = parser.parse(stats).getAsJsonObject();
            BigInteger treeDBEncryptedSum = jObj.get("sum").getAsBigInteger();
            long treeDbRetrievalTime = (System.nanoTime() - start)/1000000;

            BigInteger treeDBDecryptedSum = keys.privateKey.raw_decrypt(treeDBEncryptedSum);
            printStats("TreeDB+S3", treeDbRetrievalTime, 0);

            System.out.println("Actual SUM:  " + actualSum);
            System.out.println("S3 SUM:  " + s3Sum);
            System.out.println("TreeDB+S3 SUM: " + treeDBDecryptedSum);
            
            // Clean the state before the next repetition
            // TreeDB currently simply creates a new stream
            for (Chunk c : chunks) {
                s3.del(c, baselineBucket);
            }
        }
    }

	private static void printStats(String datastore, double avgGet, double avgDecode) {
		System.out.format("%s\t%s\t%s\t%s\n", datastore, avgGet, avgDecode, avgGet+avgDecode);
	}
}
