package ch.lubu;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ch.lubu.AvaDataEntry;
import ch.lubu.AvaDataImporter;

public class AvaData {
	public int counter = 0;

	public List<Chunk> transferData(int maxBlocksize) {
		ArrayList<Chunk> chunks = new ArrayList<>();
		Chunk curChunk = Chunk.getNewBlock(maxBlocksize);
		AvaDataImporter importer = null;
		AvaDataEntry lastEntry = null;
		for (int item = 1; item <= 10; item++) {
			try {
				importer = new AvaDataImporter("./DATA", item);
				while (importer.hasNext()) {
					AvaDataEntry entry = importer.next();
					if (curChunk.getRemainingSpace() < 10) {
						curChunk.setPrimaryAttribute(entry.time_stamp); // set primary key for the chunk
						curChunk.setSecondAttribute(entry.temp_skin); // set secondary key for the chunk
						chunks.add(curChunk);
						curChunk = Chunk.getNewBlock(maxBlocksize);
					}
					curChunk.putIotData(new Entry(entry.time_stamp, "temp_amp", entry.temp_amb));
					curChunk.putIotData(new Entry(entry.time_stamp, "temp_skin", entry.temp_skin));
					curChunk.putIotData(new Entry(entry.time_stamp, "sleep_state", entry.sleep_state));
					curChunk.putIotData(new Entry(entry.time_stamp, "avg_bpm", entry.avg_bpm));
					curChunk.putIotData(new Entry(entry.time_stamp, "activity_index", entry.activity_index));
					curChunk.putIotData(new Entry(entry.time_stamp, "accel_z", entry.accel_z));
					curChunk.putIotData(
							new Entry(entry.time_stamp, "perfusion_index_green", entry.perfusion_index_green));
					curChunk.putIotData(
							new Entry(entry.time_stamp, "perfusion_index_infrared", entry.perfusion_index_infrared));
					curChunk.putIotData(new Entry(entry.time_stamp, "phase_60kHz", entry.phase_60kHz));
					curChunk.putIotData(new Entry(entry.time_stamp, "impedance_60kHz", entry.impedance_60kHz));
					counter += 10;
					
					lastEntry = entry;
				}
				if (importer != null)
					importer.close();
			} catch (IOException e) {
				System.out.println("Ava data was not found.");
				e.printStackTrace();
			}
		}
		if (curChunk.getNumEntries() > 0) {
			curChunk.setPrimaryAttribute(lastEntry.time_stamp); // set primary key for the chunk
			curChunk.setSecondAttribute(lastEntry.temp_skin); // set secondary key for the chunk
			chunks.add(curChunk);
		}
		return chunks;
	}
}
