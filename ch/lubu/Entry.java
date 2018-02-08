package ch.lubu;

import java.io.Serializable;

public class Entry implements Serializable, Comparable<Entry> {

    private static final long serialVersionUID = 1L;

	private long timestamp;

    private int temp_skin;
    
    private byte[] data;

    public Entry(long timestamp, int temp_skin, byte[] data) {
        this.timestamp = timestamp;
        this.temp_skin = temp_skin;
        this.data = data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getValue() {
        return temp_skin;
    }
    
    public byte[] getData() {
        return data;
    }

	@Override
	public int compareTo(Entry arg0) {
		if (this.timestamp < arg0.timestamp) {
            return -1;
        } else if (this.timestamp > arg0.timestamp) {
            return 1;
        }
        return 0;
	}
}
