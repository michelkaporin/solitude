package ch.michel.test;

import java.math.BigInteger;
import java.util.Locale;
import java.util.Random;
import treedb.client.security.CryptoKeyPair;
import treedb.client.security.ECElGamalWrapper;

public class PaillierVsECElGamal {

    public static void main(String[] args) throws InterruptedException {
        CryptoKeyPair keys = CryptoKeyPair.generateKeyPair();
        ECElGamalWrapper ecelgamal = new ECElGamalWrapper();

        int timesToRun = 1000;
        float sumEncPaillier = 0;
        float sumDecPaillier = 0;
        float sumEncEcelgamal = 0;
        float sumDecEcelgamal = 0;

        /** Test memory leaks */
        System.out.println("ENC(Paillier)\tENC(ECElGamal)\tDEC(Paillier)\tDEC(ECElGamal)");
        for (int i=0; i < timesToRun; i++) {
            BigInteger plainInt = new BigInteger(32, new Random());

            /** Encrypt */
            long start = System.nanoTime();
            BigInteger paillierVal = keys.publicKey.raw_encrypt_without_obfuscation(plainInt);
            float encPaillier = timestamp(start);
            sumEncPaillier += encPaillier;

            start = System.nanoTime();
            String ecelgamalVal = ecelgamal.encryptAndEncode(plainInt);
            float encECElGamal = timestamp(start);
            sumEncEcelgamal += encECElGamal;

            /** Decrypt */
            start = System.nanoTime();
            keys.privateKey.raw_decrypt(paillierVal);
            float decPaillier = timestamp(start);
            sumDecPaillier += decPaillier;

            start = System.nanoTime();
            ecelgamal.decodeAndDecrypt(ecelgamalVal);
            float decECElGamal = timestamp(start);
            sumDecEcelgamal += decECElGamal;

            System.out.format("%s\t%s\t%s\t%s\n", format(encPaillier), format(encECElGamal), format(decPaillier), format(decECElGamal));
            //Thread.sleep(500);
        }
        System.out.format("AVG ENC (Paillier): %s\tAVG ENC(EC ElGamal): %s\tAVG DEC(Paillier): %s\tAVG DEC(EC ElGamal): %s\n", format(sumEncPaillier/timesToRun), format(sumEncEcelgamal/timesToRun), format(sumDecPaillier/timesToRun), format(sumDecEcelgamal/timesToRun));
    }
    
    private static float timestamp(long start) {
        return ((float) (System.nanoTime() - start))/1000000;
    }

    private static String format(float fl) {
        return String.format(Locale.ROOT, "%.2f", fl);
    }
}