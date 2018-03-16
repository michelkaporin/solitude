package ch.michel.test;

import ch.ethz.dsg.ecelgamal.ECElGamal;
import ch.ethz.dsg.ecelgamal.ECElGamal.CRTParams;
import ch.ethz.dsg.ecelgamal.ECElGamal.ECElGamalCiphertext;
import ch.ethz.dsg.ecelgamal.ECElGamal.ECElGamalKey;
import ch.ethz.dsg.ore.ORE;
import ch.ethz.dsg.ore.ORE.ORECiphertext;
import ch.ethz.dsg.ore.ORE.OREKey;
import java.math.BigInteger;
import java.util.Locale;
import java.util.Random;
import timecrypt.client.security.OPEWrapper;
import timecrypt.client.security.PaillierWrapper;

public class CryptoComparison {

    public static void main(String[] args) throws Exception {
        PaillierWrapper paillier = new PaillierWrapper();
        OPEWrapper ope = new OPEWrapper();

        CRTParams params64 = ECElGamal.getDefault64BitParams();
        ECElGamalKey ecelgamalKey = ECElGamal.generateNewKey(params64);
        ECElGamal.initBsgsTable(65536);

        OREKey oreKey = ORE.generateKey();
        ORE ore = ORE.getDefaultOREInstance(oreKey);

        int timesToRun = 100;

        float sumEncPaillier = 0;
        float sumDecPaillier = 0;
        float sumOperationPaillier = 0;

        float sumEncEcelgamal = 0;
        float sumDecEcelgamal = 0;
        float sumOperationEcelgamal = 0;

        float sumEncOpe = 0;
        float sumDecOpe = 0;
        float sumOperationOpe = 0;

        float sumEncOre = 0;
        float sumDecOre = 0;
        float sumOperationOre = 0;
        /** Test memory leaks */
        for (int i=0; i < timesToRun; i++) {
            BigInteger plainInt = new BigInteger(32, new Random());

            /** Encrypt */
            long start = System.nanoTime();
            BigInteger paillierVal = paillier.encrypt(plainInt);
            float encPaillier = timestamp(start);
            sumEncPaillier += encPaillier;

            start = System.nanoTime();
            ECElGamalCiphertext ecelgamalVal = ECElGamal.encrypt(plainInt, ecelgamalKey);
            float encECElGamal = timestamp(start);
            sumEncEcelgamal += encECElGamal;

            start = System.nanoTime();
            BigInteger opeVal = ope.encrypt(plainInt);
            float encOpe = timestamp(start);
            sumEncOpe += encOpe;

            start = System.nanoTime();
            ORECiphertext oreVal = ore.encrypt(plainInt.longValueExact());
            float encOre = timestamp(start);
            sumEncOre += encOre;

            /** Decrypt */
            start = System.nanoTime();
            paillier.decrypt(paillierVal);
            float decPaillier = timestamp(start);
            sumDecPaillier += decPaillier;

            start = System.nanoTime();
            ECElGamal.decrypt64(ecelgamalVal, ecelgamalKey);
            float decECElGamal = timestamp(start);
            sumDecEcelgamal += decECElGamal;

            start = System.nanoTime();
            ope.decrypt(opeVal);
            float decOpe = timestamp(start);
            sumDecOpe += decOpe;

            start = System.nanoTime();
            ore.decrypt(oreVal);
            float decOre = timestamp(start);
            sumDecOre += decOre;

            /** Operation */
            start = System.nanoTime();
            paillierVal.add(paillierVal);
            float operationPaillier = timestamp(start);
            sumOperationPaillier += operationPaillier;

            start = System.nanoTime();
            ECElGamal.add(ecelgamalVal, ecelgamalVal);
            float operationEcelgamal = timestamp(start);
            sumOperationEcelgamal += operationEcelgamal;
            
            start = System.nanoTime();
            opeVal.max(opeVal);
            float operationOpe = timestamp(start);
            sumOperationOpe += operationOpe;

            start = System.nanoTime();
            oreVal.compareTo(oreVal);
            float operationOre = timestamp(start);
            sumOperationOre += operationOre;

            System.out.format("Enc(Paillier): %s\tEnc(EC ElGamal): %s\tEnc(OPE): %s\tEnc(ORE): %s\n", format(encPaillier/timesToRun), format(encECElGamal/timesToRun), format(encOpe/timesToRun), format(encOre/timesToRun));

            System.out.format("Dec(Paillier): %s\tDec(EC ElGamal): %s\tDec(OPE): %s\tDec(ORE): %s\n", format(decPaillier/timesToRun), format(decECElGamal/timesToRun), format(decOpe/timesToRun), format(decOre/timesToRun));
    
            System.out.format("Operation(Paillier): %s\tOperation(EC ElGamal): %s\tOperation(OPE): %s\tOperation(ORE): %s\n", format(operationPaillier/timesToRun), format(operationEcelgamal/timesToRun), format(operationOpe/timesToRun), format(operationOre/timesToRun));    
        }

        System.out.println("AVERAGED");
        System.out.format("Enc(Paillier): %s\tEnc(EC ElGamal): %s\tEnc(OPE): %s\tEnc(ORE): %s\n", format(sumEncPaillier/timesToRun), format(sumEncEcelgamal/timesToRun), format(sumEncOpe/timesToRun), format(sumEncOre/timesToRun));

        System.out.format("Dec(Paillier): %s\tDec(EC ElGamal): %s\tDec(OPE): %s\tDec(ORE): %s\n", format(sumDecPaillier/timesToRun), format(sumDecEcelgamal/timesToRun), format(sumDecOpe/timesToRun), format(sumDecOre/timesToRun));

        System.out.format("Operation(Paillier): %s\tOperation(EC ElGamal): %s\tOperation(OPE): %s\tOperation(ORE): %s\n", format(sumOperationPaillier/timesToRun), format(sumOperationEcelgamal/timesToRun), format(sumOperationOpe/timesToRun), format(sumOperationOre/timesToRun));
    }
    
    private static float timestamp(long start) {
        return ((float) (System.nanoTime() - start))/1000000;
    }

    private static String format(float fl) {
        return String.format(Locale.ROOT, "%.3f", fl);
    }
}