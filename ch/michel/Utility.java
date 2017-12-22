package ch.michel;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import ch.lubu.Chunk;

public class Utility {
	
	public static List<Label> getTempLabels(List<Chunk> chunks, int labelCount) {
		TreeMap<Integer, Integer> tempToCount = new TreeMap<>(); // 13C -> 3 times
		for (Chunk c : chunks) {
			int temp = c.secondAttribute;
			Integer currentCount = tempToCount.get(temp);
			if (currentCount == null) {
				tempToCount.put(temp, 1);
			} else {
				tempToCount.put(temp, ++currentCount);
			}
		}

		List<Label> labels = new ArrayList<>();
		int chunksPerLabel = (int) Math.ceil((double) tempToCount.size() / (double) labelCount);
		int begin = 0;
		int end = chunksPerLabel;
		for (int i=0; i < labelCount; i++) {
			if (end > tempToCount.size()) {
				end = tempToCount.size();
			}
			
			int lowTemp = (Integer) tempToCount.keySet().toArray()[begin];
			int highTemp = (Integer) tempToCount.keySet().toArray()[end-1];

			labels.add(new Label(String.format("temp_l%s_h%s", lowTemp, highTemp), lowTemp,  highTemp));
			
			begin += chunksPerLabel;
			end += chunksPerLabel;
		}
		
		return labels;
	}
	
	public static SecretKey generateSecretKey() {
		KeyGenerator keyGen = null;
		try {
			keyGen = KeyGenerator.getInstance("AES");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.out.println("Failed to obtain instance of AES");
		}

		keyGen.init(128); // for example

		return keyGen.generateKey();
	}

	public static String getSpaceName(DataRepresentation representation, boolean twodimensional) {
		switch (representation) {
		case CHUNKED_COMPRESSED:
			if (twodimensional) {
				return "compressed_c2";
			}
			return "compressed_c";
		case CHUNKED_COMPRESSED_ENCRYPTED:
			if (twodimensional) {
				return "encrypted_cc2";
			}
			return "encrypted_cc";
		default:
			if (twodimensional) {
				return "chunked2";
			}
			return "chunked";
		}
	}
}
