package ch.lubu;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.lubu.AvaDataEntry;
import ch.lubu.AvaDataImporter;

public class AvaData {
	public int counter;
	
	private List<Chunk> chunks;
	private List<Chunk> tempSkinUniqueChunks = new ArrayList<Chunk>(); // Maintain map of chunks with unique second attribute
	private int cachedBlockSize = -1; // ensure correct block size is cached
	
	public List<Chunk> getChunks(int maxBlocksize, boolean tempSkinUnique) {
		if (tempSkinUnique) {
			return this.getTempSkinUniqueChunks(maxBlocksize);
		}
		
		if (this.chunks == null || this.chunks.size() == 0 || cachedBlockSize != maxBlocksize) {
			this.transferData(maxBlocksize);
		}
		
		return this.chunks;
	}
	
	private List<Chunk> getTempSkinUniqueChunks(int maxBlocksize) {
		if (this.tempSkinUniqueChunks == null || this.tempSkinUniqueChunks.size() == 0 || cachedBlockSize != maxBlocksize) {
			this.transferData(maxBlocksize);
		}
		
		return this.tempSkinUniqueChunks;
	}
	
	private void transferData(int maxBlocksize) {
		Chunk curChunk = Chunk.getNewBlock(maxBlocksize);
		Map<Integer, Chunk> tempSkinMap = new HashMap<Integer, Chunk>(); 
		AvaDataImporter importer = null;
		AvaDataEntry lastEntry = null;
		this.chunks = new ArrayList<Chunk>();
		this.tempSkinUniqueChunks = new ArrayList<Chunk>();
		this.counter = 0;
		
		for (int item = 1; item <= 10; item++) {
			try {
				importer = new AvaDataImporter("./DATA", item);
				while (importer.hasNext()) {
					AvaDataEntry entry = importer.next();
					if (curChunk.getRemainingSpace() < 10) {
						curChunk.setPrimaryAttribute(entry.time_stamp); // set primary key for the chunk
						curChunk.setSecondAttribute(entry.temp_skin); // set secondary key for the chunk
						if (tempSkinMap.get(entry.temp_skin) == null) {
							this.tempSkinUniqueChunks.add(curChunk);
							tempSkinMap.put(entry.temp_skin, curChunk);
						}
						this.chunks.add(curChunk);
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
				System.exit(1);
			}
		}
		
		if (curChunk.getNumEntries() > 0) {
			curChunk.setPrimaryAttribute(lastEntry.time_stamp); // set primary key for the chunk
			curChunk.setSecondAttribute(lastEntry.temp_skin); // set secondary key for the chunk
			if (tempSkinMap.get(lastEntry.temp_skin) == null) {
				tempSkinUniqueChunks.add(curChunk);
				tempSkinMap.put(lastEntry.temp_skin, curChunk);
			}
			this.chunks.add(curChunk);
		}
		
		this.cachedBlockSize = maxBlocksize;
	}
}
