package ch.michel;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

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

			labels.add(new Label() {
				String name = String.format("temp_l%s_h%s", lowTemp, highTemp);
				int low = lowTemp;
				int high = highTemp;
			});
			
			begin += chunksPerLabel;
			end += chunksPerLabel;

			System.out.format("First: %s, Last: %s; ", lowTemp, highTemp);
		}
		
		return labels;
	}
}
