package ch.michel.test;

import ch.lubu.AvaData;
import timecrypt.client.TimeCrypt;
import timecrypt.client.security.ECElGamalWrapper;
import timecrypt.client.security.OREWrapper;
import timecrypt.client.security.PaillierWrapper;
import timecrypt.client.security.OPEWrapper;
import ch.lubu.ChunkWrapper;
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
        boolean fakeData = Boolean.valueOf(args[6]);
        
        SecretKey secretKey = Utility.generateSecretKey();

        // Extract and duplicate data to have enough chunks
        List<ChunkWrapper> chunks = new ArrayList<ChunkWrapper>();
        if (fakeData) {
            long start = 1;
            long end = 1000;
            for (int i=0; i < 2000000; i++) { // huge load experiment
                chunks.add(new ChunkWrapper(start, end));
                start += 1000;
                end += 1000;
            }
        } else {
            AvaData avaData = new AvaData();            
            chunks = ChunkWrapper.getWrappers(avaData.getChunks(maxChunkSize, false, true)); // 30 chunks
            List<ChunkWrapper> lastChunks = chunks;
            for (int i = 0; i < 34; i++) { // copy 34 times over to get to 1030 chunks for a better statistical view
                List<ChunkWrapper> newChunks = new ArrayList<>();
                for (ChunkWrapper c : lastChunks) {
                    newChunks.add(c.copy(86400*30)); // add 30 days on top of the current data
                }
                chunks.addAll(newChunks);
                lastChunks = newChunks;
                newChunks = null;
            }
        }

        S3 s3 = new S3(aws_access_key_id, aws_secret_access_key);
        PaillierWrapper paillier = new PaillierWrapper();
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
                        streamID = timecrypt.createStream(k, "{ 'sum': true }", paillier.getPublicKey(), null);
                        break;
                    case 1: // EC ElGamal
                        streamID = timecrypt.createStream(k, "{ 'sum': true, 'algorithms': { 'sum': 'ecelgamal' } }", null, null);
                        break;
                    case 2: // OPE
                        streamID = timecrypt.createStream(k, "{ 'max': true }", null, null);
                        break;
                    case 3: // ORE
                        streamID = timecrypt.createStream(k, "{ 'max': true, 'algorithms': { 'max': 'ore' } }", null, null);
                        break;
                }

                // Populate TimeCrypt
                for (ChunkWrapper c : chunks) {
                    byte[] data = c.serialise(DR, Optional.of(secretKey));
                    BigInteger plainSum = c.getSum();

                    String metadata = null;
                    switch (j) {
                        case 0:
                            metadata = String.format("{ 'from': %s, 'to': %s, 'sum': %s }", c.getFirstEntryTimestamp(), c.getLastEntryTimestamp(), paillier.encrypt(plainSum));
                            break; 
                        case 1:
                            metadata = String.format("{ 'from': %s, 'to': %s, 'sum': '%s' }", c.getFirstEntryTimestamp(), c.getLastEntryTimestamp(), ecelgamal.encryptAndEncode(plainSum));
                            break;
                        case 2: 
                            metadata = String.format("{ 'from': %s, 'to': %s, 'max': %s }", c.getFirstEntryTimestamp(), c.getLastEntryTimestamp(), ope.encrypt(plainSum));
                            break;
                        case 3:
                            metadata = String.format("{ 'from': %s, 'to': %s, 'max': '%s' }", c.getFirstEntryTimestamp(), c.getLastEntryTimestamp(), ore.encryptAndEncode(plainSum));
                            break;
                    }

                    boolean success = timecrypt.insert(streamID, c.getPrimaryAttribute(), data, metadata);
                    if (!success){
                        System.out.println("Run into exception, stopping the insert, moving on to the next stream type.");
                        break;
                    }
                }

                // Clean state of the experiment and have a minute wait until the next run
                timecrypt.delete(streamID);
                if (!fakeData) cleanState(chunks, s3, streamID, true);
                timecrypt.closeConnection();
                Thread.sleep(60000);
            }
         }
    }

	private static void cleanState(List<ChunkWrapper> chunks, S3 s3, String bucketName, boolean deleteBucket) {
        for (ChunkWrapper c : chunks) {
            s3.del(c.getChunk(), bucketName);
        }
        if (deleteBucket) s3.deleteBucket(bucketName);        
    }
}
