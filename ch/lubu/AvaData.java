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
	private List<Chunk> tempSkinUniqueChunks; // Maintain map of chunks with unique second attribute
	private int cachedBlockSize = -1; // ensure correct block size is cached
	
	public List<Chunk> getChunks(int maxBlocksize, boolean tempSkinUnique) {
		if (tempSkinUnique) {
			return this.getTempSkinUniqueChunks(maxBlocksize);
		}
		
		if (this.chunks == null || cachedBlockSize != maxBlocksize) {
			this.transferData(maxBlocksize);
		}
		
		return this.chunks;
	}
	
	private List<Chunk> getTempSkinUniqueChunks(int maxBlocksize) {
		if (this.tempSkinUniqueChunks == null || cachedBlockSize != maxBlocksize) {
			this.transferData(maxBlocksize);
		}
		
		return this.tempSkinUniqueChunks;
	}
	
	private void transferData(int maxBlocksize) {
		AvaDataImporter importer = null;
		this.counter = 0;
		List<Entry> entries = new ArrayList<>();

		// Import entries from CSVs
		for (int csvIndex = 1; csvIndex <= 10; csvIndex++) {
			try {
				importer = new AvaDataImporter("./DATA", csvIndex);
				while (importer.hasNext()) {
					AvaDataEntry entry = importer.next();
					byte[] entryData = String.format("accel_z: %s,activity_index: %s,app_frame_no: %s,"
							+ "impedance_60kHz: %s,perfusion_index_green: %s,perfusion_index_infrared: %s,"
							+ "phase_60kHz: %s,rr_quality: %s,sleep_state: %s,temp_amb: %s,", 
							entry.accel_z, entry.activity_index, entry.app_frame_no, entry.impedance_60kHz, 
							entry.perfusion_index_green, entry.perfusion_index_infrared, entry.phase_60kHz,
							entry.rr_quality, entry.sleep_state, entry.temp_amb).getBytes(); // can be stored as JSON instead.
					entries.add(new Entry(entry.time_stamp, entry.temp_skin, entryData));
				}

				if (importer != null) {
					importer.close();
				}
				} catch (IOException e) {
					System.out.println("Ava data was not found.");
					e.printStackTrace();
					System.exit(1);
				}
				
			}
			
		// Transfer data to chunks
		Chunk curChunk = Chunk.getNewBlock(maxBlocksize);
		this.chunks = new ArrayList<Chunk>();
		this.tempSkinUniqueChunks = new ArrayList<Chunk>();
		Map<Integer, Chunk> tempSkinMap = new HashMap<Integer, Chunk>();
		Entry lastEntry = null;
		
		for (Entry entry : entries) {
			if (curChunk.getRemainingSpace() < 1) {
				addChunk(curChunk, tempSkinMap, lastEntry);
				curChunk = Chunk.getNewBlock(maxBlocksize);
			}
			curChunk.putIotData(entry);
			lastEntry = entry;
		}
		
		if (curChunk.getNumEntries() > 0) {
			addChunk(curChunk, tempSkinMap, lastEntry);
		}
		
		this.cachedBlockSize = maxBlocksize;
	}

	private void addChunk(Chunk curChunk, Map<Integer, Chunk> tempSkinMap, Entry lastEntry) {
		int lastTempSkin = lastEntry.getValue();
		curChunk.setPrimaryAttribute(lastEntry.getTimestamp()); // set primary key for the chunk
		curChunk.setSecondAttribute(lastTempSkin); // set secondary key for the chunk
		if (tempSkinMap.get(lastTempSkin) == null) {
			this.tempSkinUniqueChunks.add(curChunk);
			tempSkinMap.put(lastTempSkin, curChunk);
		}
		
		this.chunks.add(curChunk);
	}
}
