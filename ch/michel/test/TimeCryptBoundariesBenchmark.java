package ch.michel.test;

import timecrypt.client.TimeCrypt;
import timecrypt.client.security.ECElGamalWrapper;
import timecrypt.client.security.OREWrapper;
import timecrypt.client.security.PaillierWrapper;
import timecrypt.client.security.OPEWrapper;
import ch.lubu.ChunkWrapper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 
 * Running:
 * java -classpath ".:lib/aws-sdk/lib/aws-java-sdk-1.11.255.jar:lib/aws-sdk/third-party/lib/*:lib/timecrypt.client-0.0.1-SNAPSHOT-jar-with-dependencies.jar" --add-modules=java.xml.bind,java.activation ch/michel/test/TimeCryptPerformance 1000 1 AKIAI6DALQXSPRIEAPWQ zerBOTixXhgqvJuij00evoJOOHYDAuYR9cYTZXYy 127.0.0.1 8001
 * 
 * Compiling:
 * javac -classpath ".:/usr/local/share/java/org.hyperdex.client-1.8.1.jar:lib/aws-sdk/lib/aws-java-sdk-1.11.255.jar:lib/aws-sdk/third-party/lib/*:lib/cassandra/cassandra-driver-core-3.3.2.jar:lib/cassandra/lib/*:lib/timecrypt.client-0.0.1-SNAPSHOT-jar-with-dependencies.jar" $(find . -name '*.java')
 */
public class TimeCryptBoundariesBenchmark {
    private static int k = 16;

    public static void main(String[] args) throws Exception {
        String timecryptIP = args[0];
        int timecryptPort = Integer.valueOf(args[1]);
        int algorithmID = Integer.valueOf(args[2]);
        
        // Extract and duplicate data to have enough chunks
        List<ChunkWrapper> chunks = new ArrayList<ChunkWrapper>();
        long start = 1;
        long end = 1000;
        for (int i=0; i < 2000000; i++) { // huge load experiment
            chunks.add(new ChunkWrapper(start, end));
            start += 1000;
            end += 1000;
        }

        PaillierWrapper paillier = new PaillierWrapper();
        ECElGamalWrapper ecelgamal = new ECElGamalWrapper();
        OREWrapper ore = new OREWrapper();
        OPEWrapper ope = new OPEWrapper();

        TimeCrypt timecrypt = new TimeCrypt(timecryptIP, timecryptPort);
        timecrypt.openConnection();

        String streamID = null;
        switch (algorithmID) { // 1: Paillier, 2: EC ElGamal, 3: OPE, 4: ORE
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
            byte[] data = c.serialise(null, null);
            BigInteger plainSum = c.getSum();

            String metadata = null;
            switch (algorithmID) {
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

         for (int j = 0; j < chunks.size(); j++) {
            switch (algorithmID) {
                case 0:
                    // GET Paillier sum
                    start = System.nanoTime();
                    String stats = timecrypt.getStatistics(streamID, chunks.get(0).getFirstEntryTimestamp(), chunks.get(j).getLastEntryTimestamp());
                    float treeDbRetrievalTime = timestamp(start);
                    
                    start = System.nanoTime();
                    JsonParser parser = new JsonParser();
                    JsonObject jObj = parser.parse(stats).getAsJsonObject();
                    BigInteger decryptedSum = paillier.decrypt(jObj.get("sum").getAsBigInteger());
                    float treeDbDecodeTime = timestamp(start);

                    print("Paillier", treeDbRetrievalTime, treeDbDecodeTime, 0);
                    break;
                case 1:
                    // GET EC El Gamal Sum
                    start = System.nanoTime();
                    stats = timecrypt.getStatistics(streamID, chunks.get(0).getFirstEntryTimestamp(), chunks.get(j).getLastEntryTimestamp());
                    treeDbRetrievalTime = timestamp(start);
                    
                    start = System.nanoTime();
                    parser = new JsonParser();
                    jObj = parser.parse(stats).getAsJsonObject();
                    BigInteger decryptedSum2 = ecelgamal.decodeAndDecrypt(jObj.get("sum").getAsString());
                    treeDbDecodeTime = timestamp(start);

                    print("EC ElGamal", treeDbRetrievalTime, treeDbDecodeTime, 0);
                    break;
                case 2:
                    // GET OPE Max
                    start = System.nanoTime();
                    stats = timecrypt.getStatistics(streamID, chunks.get(0).getFirstEntryTimestamp(), chunks.get(j).getLastEntryTimestamp());
                    treeDbRetrievalTime = timestamp(start);
                    
                    start = System.nanoTime();
                    parser = new JsonParser();
                    jObj = parser.parse(stats).getAsJsonObject();
                    BigInteger decryptedMax = ope.decrypt(jObj.get("max").getAsBigInteger());
                    treeDbDecodeTime = timestamp(start);

                    print("OPE", treeDbRetrievalTime, treeDbDecodeTime, 0);
                    break;
                case 3:
                    // GET ORE Max
                    start = System.nanoTime();
                    stats = timecrypt.getStatistics(streamID, chunks.get(0).getFirstEntryTimestamp(), chunks.get(j).getLastEntryTimestamp());
                    treeDbRetrievalTime = timestamp(start);
                    
                    start = System.nanoTime();
                    parser = new JsonParser();
                    jObj = parser.parse(stats).getAsJsonObject();
                    BigInteger decryptedMax2 = ore.decodeAndDecrypt(jObj.get("max").getAsString());
                    treeDbDecodeTime = timestamp(start);

                    print("ORE", treeDbRetrievalTime, treeDbDecodeTime, 0);
                    break;
            }
        }

        timecrypt.delete(streamID);
        timecrypt.closeConnection();
    }

    private static float timestamp(long start) {
        return ((float) (System.nanoTime() - start))/1000000;
    }

    private static void print(String design, float retrievalTime, float decodeDecryptTime, float computationTime) {
        float overall = retrievalTime+decodeDecryptTime+computationTime;

        System.out.format("%s\t%s\t%s\t%s\t%s\n", design, String.format(Locale.ROOT, "%.2f", retrievalTime), String.format(Locale.ROOT, "%.2f", decodeDecryptTime), String.format(Locale.ROOT, "%.2f", computationTime), String.format(Locale.ROOT, "%.2f", overall));

    }
}
