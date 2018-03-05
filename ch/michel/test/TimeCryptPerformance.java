package ch.michel.test;

import ch.lubu.AvaData;
import timecrypt.client.TimeCrypt;
import timecrypt.client.security.CryptoKeyPair;
import timecrypt.client.security.ECElGamalWrapper;
import timecrypt.client.security.OREWrapper;
import timecrypt.client.security.OPEWrapper;
import ch.lubu.Chunk;
import ch.michel.DataRepresentation;
import ch.michel.S3;
import ch.michel.Utility;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.crypto.SecretKey;

/**
 * 
 * Running:
 * java -classpath ".:lib/aws-sdk/lib/aws-java-sdk-1.11.255.jar:lib/aws-sdk/third-party/lib/*:lib/timecrypt.client-0.0.1-SNAPSHOT-jar-with-dependencies.jar" --add-modules=java.xml.bind,java.activation ch/michel/test/TimeCryptPerformance 1000 1 AKIAI6DALQXSPRIEAPWQ zerBOTixXhgqvJuij00evoJOOHYDAuYR9cYTZXYy 127.0.0.1 8001
 * 
 * Compiling:
 * javac -classpath ".:/usr/local/share/java/org.hyperdex.client-1.8.1.jar:lib/aws-sdk/lib/aws-java-sdk-1.11.255.jar:lib/aws-sdk/third-party/lib/*:lib/cassandra/cassandra-driver-core-3.3.2.jar:lib/cassandra/lib/*:lib/timecrypt.client-0.0.1-SNAPSHOT-jar-with-dependencies.jar" $(find . -name '*.java')
 */
public class TimeCryptPerformance {
    private static DataRepresentation DR = DataRepresentation.CHUNKED_COMPRESSED_ENCRYPTED;
    private static int k = 2;

    public static void main(String[] args) throws Exception {
        int maxChunkSize = Integer.valueOf(args[0]);
		int experimentReps = Integer.valueOf(args[1]);
		String aws_access_key_id = args[2];
        String aws_secret_access_key = args[3];
        String timecryptIP = args[4];
        int timecryptPort = Integer.valueOf(args[5]);
        SecretKey secretKey = Utility.generateSecretKey();

        // Extract and duplicate data to have enough chunks
        AvaData avaData = new AvaData();
        List<Chunk> chunks = avaData.getChunks(maxChunkSize, false, true); // 30 chunks
        List<Chunk> lastChunks = chunks;
        for (int i = 0; i < 34; i++) { // copy 34 times over to get to 1030 chunks for a better statistical view
            List<Chunk> newChunks = new ArrayList<>();
            lastChunks.forEach(c -> newChunks.add(c.copy(86400*30))); // add 30 days on top of the current data
            chunks.addAll(newChunks); 
            lastChunks = newChunks;
        }

        S3 s3 = new S3(aws_access_key_id, aws_secret_access_key);
        CryptoKeyPair keys = CryptoKeyPair.generateKeyPair();
        ECElGamalWrapper ecelgamal = new ECElGamalWrapper();
        OREWrapper ore = new OREWrapper();
        OPEWrapper ope = new OPEWrapper();

        /**
         * 1. Create stream [paillier, ecelgamal, ope, ore]
         * 2. Run experiment
         * 3. Delete stream, run another experiment over different stream starting from 1
         */
         for (int i=0; i < experimentReps; i++) {
            System.out.println("Experiment #" + i);

            for (int j=0; j < 4; j++) {
                TimeCrypt timecrypt = new TimeCrypt(timecryptIP, timecryptPort);
                timecrypt.openConnection();

                String streamID = null;
                switch (j) {
                    case 0: // Paillier 
                        streamID = timecrypt.createStream(k, "{ 'sum': true }", keys.publicKey, "S3");
                        break;
                    case 1: // EC ElGamal
                        streamID = timecrypt.createStream(k, "{ 'sum': true, 'algorithms': { 'sum': 'ecelgamal' } }", null, "S3");
                        break;
                    case 2: // OPE
                        streamID = timecrypt.createStream(k, "{ 'max': true }", null, "S3");
                        break;
                    case 3: // ORE
                        streamID = timecrypt.createStream(k, "{ 'max': true, 'algorithms': { 'max': 'ore' } }", null, "S3");
                        break;
                }

                // Populate TimeCrypt
                for (Chunk c : chunks) {
                    byte[] data = c.serialise(DR, Optional.of(secretKey));
                    BigInteger plainSum = c.getSum();

                    String metadata = null;
                    switch (j) {
                        case 0:
                            metadata = String.format("{ 'from': %s, 'to': %s, 'sum': %s }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), keys.publicKey.raw_encrypt(plainSum));
                            break; 
                        case 1:
                            metadata = String.format("{ 'from': %s, 'to': %s, 'sum': '%s' }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), ecelgamal.encryptAndEncode(plainSum));
                            break;
                        case 2: 
                            metadata = String.format("{ 'from': %s, 'to': %s, 'max': %s }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), ope.encrypt(plainSum));
                            break;
                        case 3:
                            metadata = String.format("{ 'from': %s, 'to': %s, 'max': '%s' }", c.getFirstEntry().getTimestamp(), c.getLastEntry().getTimestamp(), ore.encryptAndEncode(plainSum));
                            break;
                    }
                    timecrypt.insert(streamID, c.getPrimaryAttribute(), data, metadata);
                }

                // Clean state of the experiment
                timecrypt.delete(streamID);
                cleanState(chunks, s3, streamID, true);
                timecrypt.closeConnection();
            }
         }
    }

	private static void cleanState(List<Chunk> chunks, S3 s3, String bucketName, boolean deleteBucket) {
        for (Chunk c : chunks) {
            s3.del(c, bucketName);
        }
        if (deleteBucket) s3.deleteBucket(bucketName);        
    }
}
