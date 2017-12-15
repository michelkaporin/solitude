package ch.lubu;

import javax.crypto.*;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.Deflater;

/**
 * Represents a data block containing data
 */
public class Chunk implements Iterator<Entry>, Iterable<Entry>{

    private int maxEntries;

    private List<Entry> entries = new ArrayList<>();
    
    private long timestamp; // identifies the timestamp of the last entry in the chunk
    public int secondAttribute; // specifies second attribute to query that chunk
    
    private byte[] data;
    private byte[] compressed;
    private byte[] compressedAndEncrypted;

    private Chunk(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    public static Chunk getNewBlock(int maxEntries) {
        return new Chunk(maxEntries);
    }

    private int getSizeEntry(Entry entry) {
        return 8 + 4 + entry.getMetadata().length() * 2 + 8;
    }

    private byte[] entryToByte(Entry entry) {
        int len = entry.getMetadata().length();
        int lenTot = getSizeEntry(entry);
        ByteBuffer data = ByteBuffer.allocate(lenTot);
        data.putLong(entry.getTimestamp());
        data.putInt(len);
        for (char v : entry.getMetadata().toCharArray())
            data.putChar(v);
        data.putDouble(entry.getValue());
        return data.array();
    }

    /**
     * Adds Iot data to a block
     * @param entry
     * @return
     */
    public boolean putIotData(Entry entry) {
        if (this.entries.size() >= maxEntries)
            return false;
        this.entries.add(entry);
        return true;
    }
    
    public String getPrimaryAttribute() {	
    		// A key in HyperDex is a string due to long and Timestamp types are not supported by the Java bindings
    		return String.valueOf(this.timestamp);
    }
    
    public void setPrimaryAttribute(long time) {
		this.timestamp = time;
    }
    
    public void setSecondAttribute(int attribute) {
    		this.secondAttribute = attribute;
    }

    public byte[] getData() {
    		if (data != null) {
    			return data;
    		}
    		
        int len_tot = 0;
        for (Entry item : entries)
            len_tot += getSizeEntry(item);
        int cur_index = 0;
        byte[] tot = new byte[len_tot];
        for (Entry item : entries) {
            byte[] tmp = entryToByte(item);
            System.arraycopy(tmp, 0, tot, cur_index, tmp.length);
            cur_index += tmp.length;
        }
        data = tot;
        
        return tot;
    }

    public byte[] getCompressedData() {
    		if (compressed != null) {
    			return compressed;
    		}
    		
        Deflater compresser = new Deflater();
        byte[] input = getData();
        byte[] output_buffer = new byte[input.length];
        compresser.setInput(input);
        compresser.finish();
        int numBytes = compresser.deflate(output_buffer);
        byte[] result = new byte[numBytes];
        System.arraycopy(output_buffer, 0, result, 0, numBytes);
        compressed = result;
        
        return result;
    }

    public byte[] getCompressedAndEncryptedData(byte[] key) {
    		if (compressedAndEncrypted != null) {
    			return compressedAndEncrypted;
    		}
    		
    		byte[] finalResult = null;
	    	try {
	        SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
	
	        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
	        SecureRandom randomSecureRandom = new SecureRandom();
	        byte[] ivBytes = new byte[cipher.getBlockSize()];
	        randomSecureRandom.nextBytes(ivBytes);
	
	        IvParameterSpec iv = new IvParameterSpec(ivBytes);
	
	        cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
	
	        byte[] compressedData = getCompressedData();
	        byte[] encrypted = cipher.doFinal(compressedData);
	        finalResult = new byte[ivBytes.length + encrypted.length];
	        System.arraycopy(ivBytes, 0, finalResult, 0, ivBytes.length);
	        System.arraycopy(encrypted,0, finalResult, ivBytes.length, encrypted.length);
	    	} catch (Exception e) {
	    		System.out.println("Failed to compress and encrypt the chunk.");
	    	}
	    	compressedAndEncrypted = finalResult;
	    	
        return finalResult;
    }

    public byte[] getCompressedAndEncryptedGCMData(byte[] key) throws Exception {
        SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
        int nonceSize = 12;

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        SecureRandom randomSecureRandom = new SecureRandom();
        byte[] ivBytes = new byte[nonceSize];
        randomSecureRandom.nextBytes(ivBytes);

        GCMParameterSpec gcm = new GCMParameterSpec(cipher.getBlockSize() * 8, ivBytes);

        cipher.init(Cipher.ENCRYPT_MODE, skeySpec, gcm);

        byte[] compressedData = getCompressedData();
        byte[] encrypted = cipher.doFinal(compressedData);
        byte[] finalResult = new byte[ivBytes.length + encrypted.length];
        System.arraycopy(ivBytes, 0, finalResult, 0, ivBytes.length);
        System.arraycopy(encrypted,0, finalResult, ivBytes.length, encrypted.length);
        return finalResult;
    }

    public byte[] getCompressedEncryptedSignedData(byte[] key, PrivateKey privECDSA, boolean useGCM) throws Exception {
        Signature sig = Signature.getInstance("SHA256withECDSA");
        sig.initSign(privECDSA);
        byte[] data = null;
        if(useGCM)
            data = getCompressedAndEncryptedGCMData(key);
        else
            data = getCompressedAndEncryptedData(key);
        sig.update(data);
        byte[] signature = sig.sign();
        ByteBuffer buff = ByteBuffer.allocate(data.length + signature.length + Integer.BYTES);
        buff.putInt(signature.length);
        buff.put(signature);
        buff.put(data);
        return buff.array();
    }

    private int curID = -1;

    @Override
    public boolean hasNext() {
        return curID + 1 < entries.size();
    }

    @Override
    public Entry next() {
        if(hasNext()) {
            curID ++;
            return entries.get(curID);
        } else {
            return null;
        }
    }

    @Override
    public Iterator<Entry> iterator() {
        return this;
    }

    public int getRemainingSpace() {
        return this.maxEntries - entries.size();
    }

    public int getNumEntries() {
        return entries.size();
    }
}
