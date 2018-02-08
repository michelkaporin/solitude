package ch.lubu;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import ch.michel.DataRepresentation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.security.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Represents a data block containing data
 */
public class Chunk implements Iterator<Entry>, Iterable<Entry> {

    private int maxEntries;

    private List<Entry> entries = new ArrayList<>();
    
    private long timestamp; // identifies the timestamp of the last entry in the chunk
    public int secondAttribute; // specifies second attribute to query that chunk
    
    private byte[] data;
    private byte[] compressed;
    private byte[] compressedAndEncrypted;

    protected Chunk(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    public static Chunk getNewBlock(int maxEntries) {
        return new Chunk(maxEntries);
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

    public BigInteger getSum() {
        BigInteger sum = BigInteger.ZERO;
        for (Entry entry : this.entries) {
            sum = sum.add(BigInteger.valueOf(entry.getValue()));
        }
        return sum;
    }

    public BigInteger getCount() {
        // TODO
        return null;
    }

    public BigInteger getMin() {
        // TODO
        return null;
    }
    
    public BigInteger getMax() {
        // TODO
        return null;
    }

    public Entry getFirstEntry() {
        return this.entries.get(0);
    }
    public Entry getLastEntry() {
        return this.entries.get(this.entries.size()-1);
    }
    
    public byte[] serialise(DataRepresentation state, Optional<SecretKey> secretKey) {
		byte[] data = null;
		
		switch (state) {
			case CHUNKED_COMPRESSED:
				data = this.getCompressedData();
				break;
			case CHUNKED_COMPRESSED_ENCRYPTED:
				SecretKey key = null;
				if (secretKey.isPresent()) {
					key = secretKey.get();
				} else {
					break;
				}
	
				try {
					data = this.getCompressedAndEncryptedData(key.getEncoded());
				} catch (Exception e1) {
					e1.printStackTrace();
					System.out.println("Failed to obtain secret key");
				}
				break;
			default:
				data = this.getData();
				break;
		}
		
		return data;
    }

    public static Chunk deserialise(DataRepresentation state, byte[] encodedData, Optional<SecretKey> secretKey) {
        byte[] data = null;

        switch (state) {
            case CHUNKED_COMPRESSED:
                data = getDecompressedData(encodedData);
                break;
            case CHUNKED_COMPRESSED_ENCRYPTED:
                if (secretKey.isPresent()) {
                    data = getDecompressedEncrypted(encodedData, secretKey.get().getEncoded());
                } else {
                		System.out.println("Secret key to decrypt chunk was not provided.");
                    System.exit(1);
                }
                break;
            default:
                data = encodedData;
                break;
        }
        
        ObjectInputStream ois = null;
        SerialisableChunk schunk = null;
		try {
			ois = new ObjectInputStream(new ByteArrayInputStream(data));
			schunk = (SerialisableChunk) ois.readObject();

		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Failed to deserialise the chunk");
			e.printStackTrace();
			System.exit(1);
		}
		
        Chunk chunk = new Chunk(schunk.entries.size());
        chunk.entries = schunk.entries;
        
        return chunk;
    }

    private byte[] getData() {
    		if (data != null) {
    			return data;
    		}
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(bos);
	        SerialisableChunk schunk = new SerialisableChunk();
	        schunk.entries = this.entries;
	        
	        oos.writeObject(schunk);
	        oos.flush();
	        
	        data = bos.toByteArray();
			bos.close();
		} catch (IOException e) {
			System.out.println("Failed to serialise object");
			e.printStackTrace();
			System.exit(1);
		}
		
        return data;
    }

    private byte[] getCompressedData() {
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

    private static byte[] getDecompressedData(byte[] compressedData) {
        Inflater decompresser = new Inflater();
        decompresser.setInput(compressedData, 0, compressedData.length);
        byte[] output_buffer = new byte[compressedData.length * 100];
        int resultLength = 0;
		try {
			resultLength = decompresser.inflate(output_buffer);
		} catch (DataFormatException e) {
			System.out.println("Failed to decompress the chunk data");
			e.printStackTrace();
		}
        decompresser.end();
        byte[] result = new byte[resultLength];
        System.arraycopy(output_buffer, 0, result, 0, resultLength);
        
        return result;
    }

    private byte[] getCompressedAndEncryptedData(byte[] key) {
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
    
    private static byte[] getDecompressedEncrypted(byte[] compressedEncryptedChunk, byte[] key) {
        SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
        Cipher cipher = null;
        try {
            cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
            byte[] ivByte = new byte[cipher.getBlockSize()];
            System.arraycopy(compressedEncryptedChunk, 0, ivByte, 0, ivByte.length);

            IvParameterSpec iv = new IvParameterSpec(ivByte);
            cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
            byte[] compressedData = cipher.doFinal(compressedEncryptedChunk, ivByte.length, compressedEncryptedChunk.length - ivByte.length);
            return getDecompressedData(compressedData);
        } catch (Exception e) {
        		System.out.println("Something went wrong when decompressing encrypted data");
            e.printStackTrace();
        }
        
        return null;
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
