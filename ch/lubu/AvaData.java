package ch.lubu;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.lubu.AvaDataEntry;
import ch.lubu.AvaDataImporter;
import ch.michel.Label;

public class AvaData {
	public int counter;
	
	private List<Chunk> chunks;
	private List<Chunk> tempSkinUniqueChunks; // Maintain map of chunks with unique second attribute
	private int cachedBlockSize = -1; // ensure correct block size is cached
	
	public List<Chunk> getChunks(int maxBlocksize, boolean tempSkinUnique, boolean sortEntries) {
		if (tempSkinUnique) {
			return this.getTempSkinUniqueChunks(maxBlocksize);
		}
		
		if (this.chunks == null || cachedBlockSize != maxBlocksize) {
			this.transferData(maxBlocksize, sortEntries);
		}
		
		return this.chunks;
	}
	
	public List<Chunk> getLabelledChunks(int maxBlocksize, Label label) {
		List<Entry> entries = importEntries(false);
		List<Chunk> chunks = new ArrayList<>();
		
		Chunk curChunk = Chunk.getNewBlock(maxBlocksize);
		Entry lastEntry = null;

		for (Entry entry : entries) {
			if (curChunk.getRemainingSpace() < 1) {
				curChunk.setPrimaryAttribute(lastEntry.getTimestamp()); // set primary key for the chunk
				chunks.add(curChunk);
				curChunk = Chunk.getNewBlock(maxBlocksize);
			}
			if (entry.getValue() > label.low && entry.getValue() < label.high) {
				curChunk.putIotData(entry);
				lastEntry = entry;
			}
		}

		if (curChunk.getNumEntries() > 0) {
			curChunk.setPrimaryAttribute(lastEntry.getTimestamp()); // set primary key for the chunk
			chunks.add(curChunk);
		}
		
		return chunks;
	}
	
	private List<Chunk> getTempSkinUniqueChunks(int maxBlocksize) {
		if (this.tempSkinUniqueChunks == null || cachedBlockSize != maxBlocksize) {
			this.transferData(maxBlocksize, false);
		}
		
		return this.tempSkinUniqueChunks;
	}
	
	private void transferData(int maxBlocksize, boolean sortEntries) {
		this.counter = 0;

		// Import entries from CSVs
		List<Entry> entries = importEntries(sortEntries);
		if (sortEntries) Collections.sort(entries);

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
			this.counter +=1;
			lastEntry = entry;
		}
		
		if (curChunk.getNumEntries() > 0) {
			addChunk(curChunk, tempSkinMap, lastEntry);
		}
		
		this.cachedBlockSize = maxBlocksize;
	}

	private List<Entry> importEntries(boolean sortEntries) {
		List<Entry> entries = new ArrayList<>();
		
		AvaDataImporter importer;
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
		
		return entries;
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
