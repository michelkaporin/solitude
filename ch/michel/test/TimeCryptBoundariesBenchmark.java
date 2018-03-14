package ch.michel.test;

import timecrypt.client.TimeCrypt;
import timecrypt.client.security.ECElGamalWrapper;
import timecrypt.client.security.OREWrapper;
import timecrypt.client.security.PaillierWrapper;
import timecrypt.client.security.OPEWrapper;
import ch.lubu.ChunkWrapper;
import ch.michel.Utility;
import ch.michel.test.timecrypt.BaselineMetadata;
import ch.michel.test.timecrypt.TimeCryptBaselineClient;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import javax.crypto.SecretKey;

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
        String unparsedAlgorithmIDs = args[2];
        List<Integer> algorithmIDs = Arrays.stream(unparsedAlgorithmIDs.split(",")).map(Integer::parseInt)
                .collect(Collectors.toList());
        ;

        String trdbBaselineIP = null;
        int trdbBaselinePort = 8002;
        try {
            trdbBaselineIP = args[3];
            trdbBaselinePort = Integer.valueOf(args[4]);
        } catch (Exception e) {
        }

        // Extract and duplicate data to have enough chunks
        List<ChunkWrapper> chunks = new ArrayList<ChunkWrapper>();
        long start = 1;
        long end = 1000;
        for (int i = 0; i < 1500000; i++) { // huge load experiment
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

        TimeCryptBaselineClient trdbBaseline = null;
        SecretKey secretKey = null;
        for (int algorithmID : algorithmIDs) {
            if (algorithmID == 4) {
                secretKey = Utility.generateSecretKey();
                trdbBaseline = new TimeCryptBaselineClient(trdbBaselineIP, trdbBaselinePort);
                trdbBaseline.openConnection();
            }

            String streamID = null;
            switch (algorithmID) { // 1: Paillier, 2: EC ElGamal, 3: OPE, 4: ORE
            case 0: // Paillier 
                streamID = timecrypt.createStream(k, "{ 'sum': true }", paillier.getPublicKey(), null);
                break;
            case 1: // EC ElGamal
                streamID = timecrypt.createStream(k, "{ 'sum': true, 'algorithms': { 'sum': 'ecelgamal' } }", null,
                        null);
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
                    metadata = String.format("{ 'from': %s, 'to': %s, 'sum': %s }", c.getFirstEntryTimestamp(),
                            c.getLastEntryTimestamp(), paillier.encrypt(plainSum));
                    break;
                case 1:
                    metadata = String.format("{ 'from': %s, 'to': %s, 'sum': '%s' }", c.getFirstEntryTimestamp(),
                            c.getLastEntryTimestamp(), ecelgamal.encryptAndEncode(plainSum));
                    break;
                case 2:
                    metadata = String.format("{ 'from': %s, 'to': %s, 'max': %s }", c.getFirstEntryTimestamp(),
                            c.getLastEntryTimestamp(), ope.encrypt(plainSum));
                    break;
                case 3:
                    metadata = String.format("{ 'from': %s, 'to': %s, 'max': '%s' }", c.getFirstEntryTimestamp(),
                            c.getLastEntryTimestamp(), ore.encryptAndEncode(plainSum));
                    break;
                case 4: // Baseline sum
                    metadata = new BaselineMetadata(
                            String.format("{ 'sum': %s, 'max': %s }", plainSum.toString(), plainSum.toString()))
                                    .getEncodedEncryptedData(secretKey.getEncoded());
                    break;
                case 5: // Baseline max
                    metadata = new BaselineMetadata(
                            String.format("{ 'max': %s }", plainSum.toString(), plainSum.toString()))
                                    .getEncodedEncryptedData(secretKey.getEncoded());
                    break;
                }

                boolean success = true;
                if (algorithmID == 4 || algorithmID == 5) {
                    trdbBaseline.insert(c.getFirstEntryTimestamp(), c.getLastEntryTimestamp(), metadata);
                } else {
                    success = timecrypt.insert(streamID, c.getPrimaryAttribute(), data, metadata);
                }
                if (!success) {
                    System.out.println("Run into exception, stopping the insert, moving on to the next stream type.");
                    break;
                }
            }

            for (int j = 0; j < chunks.size(); j++) {
                switch (algorithmID) {
                case 0: {
                    // GET Paillier sum
                    start = System.nanoTime();
                    String stats = timecrypt.getStatistics(streamID, chunks.get(0).getFirstEntryTimestamp(),
                            chunks.get(j).getLastEntryTimestamp());
                    float treeDbRetrievalTime = timestamp(start);

                    start = System.nanoTime();
                    JsonParser parser = new JsonParser();
                    JsonObject jObj = parser.parse(stats).getAsJsonObject();
                    BigInteger decryptedSum = paillier.decrypt(jObj.get("sum").getAsBigInteger());
                    float treeDbDecodeTime = timestamp(start);

                    print("Paillier", treeDbRetrievalTime, treeDbDecodeTime, 0);
                    break;
                }
                case 1: {
                    // GET EC El Gamal Sum
                    start = System.nanoTime();
                    String stats = timecrypt.getStatistics(streamID, chunks.get(0).getFirstEntryTimestamp(),
                            chunks.get(j).getLastEntryTimestamp());
                    float treeDbRetrievalTime = timestamp(start);

                    start = System.nanoTime();
                    JsonParser parser = new JsonParser();
                    JsonObject jObj = parser.parse(stats).getAsJsonObject();
                    BigInteger decryptedSum2 = ecelgamal.decodeAndDecrypt(jObj.get("sum").getAsString());
                    float treeDbDecodeTime = timestamp(start);

                    print("EC ElGamal", treeDbRetrievalTime, treeDbDecodeTime, 0);
                    break;
                }
                case 2: {
                    // GET OPE Max
                    start = System.nanoTime();
                    String stats = timecrypt.getStatistics(streamID, chunks.get(0).getFirstEntryTimestamp(),
                            chunks.get(j).getLastEntryTimestamp());
                    float treeDbRetrievalTime = timestamp(start);

                    start = System.nanoTime();
                    JsonParser parser = new JsonParser();
                    JsonObject jObj = parser.parse(stats).getAsJsonObject();
                    BigInteger decryptedMax = ope.decrypt(jObj.get("max").getAsBigInteger());
                    float treeDbDecodeTime = timestamp(start);

                    print("OPE", treeDbRetrievalTime, treeDbDecodeTime, 0);
                    break;
                }
                case 3: {
                    // GET ORE Max
                    start = System.nanoTime();
                    String stats = timecrypt.getStatistics(streamID, chunks.get(0).getFirstEntryTimestamp(),
                            chunks.get(j).getLastEntryTimestamp());
                    float treeDbRetrievalTime = timestamp(start);

                    start = System.nanoTime();
                    JsonParser parser = new JsonParser();
                    JsonObject jObj = parser.parse(stats).getAsJsonObject();
                    BigInteger decryptedMax2 = ore.decodeAndDecrypt(jObj.get("max").getAsString());
                    float treeDbDecodeTime = timestamp(start);

                    print("ORE", treeDbRetrievalTime, treeDbDecodeTime, 0);
                    break;
                }
                case 4: {
                    // GET Baseline SUM
                    start = System.nanoTime();
                    List<byte[]> metadataList = trdbBaseline.getRange(chunks.get(0).getFirstEntryTimestamp(),
                            chunks.get(j).getLastEntryTimestamp());
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

                    print("TimeCrypt Baseline SUM", retrievalTime, decodeDecryptTime, sumComputationTime);
                    break;
                }
                case 5: {
                    // GET Baseline SUM
                    start = System.nanoTime();
                    List<byte[]> metadataList = trdbBaseline.getRange(chunks.get(0).getFirstEntryTimestamp(),
                            chunks.get(j).getLastEntryTimestamp());
                    float retrievalTime = timestamp(start);

                    List<String> metadataStrings = new ArrayList<>();
                    start = System.nanoTime();
                    for (byte[] md : metadataList) {
                        String metadata = BaselineMetadata.decryptMetadata(md, secretKey.getEncoded());
                        metadataStrings.add(metadata);
                    }
                    float decodeDecryptTime = timestamp(start);

                    // Compute max
                    start = System.nanoTime();
                    JsonParser parser = new JsonParser();
                    BigInteger max = BigInteger.ZERO;
                    for (String md : metadataStrings) {
                        JsonObject jObj = parser.parse(md).getAsJsonObject();
                        max = max.max(jObj.get("max").getAsBigInteger());
                    }
                    float maxComputationTime = timestamp(start);

                    print("TimeCrypt Baseline Max", retrievalTime, decodeDecryptTime, maxComputationTime);
                    break;
                }
                }
            }

            if (algorithmID == 4 || algorithmID == 5) {
                trdbBaseline.clean();
            } else {
                timecrypt.delete(streamID);
            }
        }

        timecrypt.closeConnection();
        if (trdbBaseline != null)
            trdbBaseline.closeConnection();
    }

    private static float timestamp(long start) {
        return ((float) (System.nanoTime() - start)) / 1000000;
    }

    private static void print(String design, float retrievalTime, float decodeDecryptTime, float computationTime) {
        float overall = retrievalTime + decodeDecryptTime + computationTime;

        System.out.format("%s\t%s\t%s\t%s\t%s\n", design, String.format(Locale.ROOT, "%.2f", retrievalTime),
                String.format(Locale.ROOT, "%.2f", decodeDecryptTime),
                String.format(Locale.ROOT, "%.2f", computationTime), String.format(Locale.ROOT, "%.2f", overall));
    }
}
