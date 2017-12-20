package ch.lubu;

/**
 * Created by lukas on 24.02.17.
 */
public class Entry {

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
}
