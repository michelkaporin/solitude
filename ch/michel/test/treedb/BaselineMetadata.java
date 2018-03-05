package ch.michel.test.treedb;

import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.amazonaws.util.Base64;

public class BaselineMetadata {

    private String metadata;

    public BaselineMetadata(String metadata) {
        this.metadata = metadata;
    }

    public static String decryptMetadata(byte[] data, byte[] key) {
        SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
        Cipher cipher = null;
        try {
            cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
            byte[] ivByte = new byte[cipher.getBlockSize()];
            System.arraycopy(data, 0, ivByte, 0, ivByte.length);

            IvParameterSpec iv = new IvParameterSpec(ivByte);
            cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
            byte[] decryptedData = cipher.doFinal(data, ivByte.length, data.length - ivByte.length);
            return new String(decryptedData);
        } catch (Exception e) {
            System.out.println("Something went wrong when decrypting the data");
            e.printStackTrace();
        }
        return null;
    }

    public String getEncodedEncryptedData(byte[] key) {
        return Base64.encodeAsString(this.getEncryptedData(key));
    }

    private byte[] getEncryptedData(byte[] key) {
        byte[] finalResult = null;
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");

            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
            SecureRandom randomSecureRandom = new SecureRandom();
            byte[] ivBytes = new byte[cipher.getBlockSize()];
            randomSecureRandom.nextBytes(ivBytes);

            IvParameterSpec iv = new IvParameterSpec(ivBytes);
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);

            byte[] encrypted = cipher.doFinal(metadata.getBytes());
            finalResult = new byte[ivBytes.length + encrypted.length];
            System.arraycopy(ivBytes, 0, finalResult, 0, ivBytes.length);
            System.arraycopy(encrypted, 0, finalResult, ivBytes.length, encrypted.length);
        } catch (Exception e) {
            System.out.println("Failed to compress and encrypt the chunk.");
        }

        return finalResult;
    }
}