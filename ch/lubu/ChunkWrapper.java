package ch.lubu;

import ch.michel.DataRepresentation;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.crypto.SecretKey;

/**
 * Used to allow fake chunks creation 
 */
public class ChunkWrapper {
    private Chunk chunk;

    private long firstEntryTimestamp;
    private long lastEntryTimestamp;

    public ChunkWrapper(long firstEntryTimestamp, long lastEntryTimestamp) {
        this.firstEntryTimestamp = firstEntryTimestamp;
        this.lastEntryTimestamp = lastEntryTimestamp;
    }

    public ChunkWrapper(Chunk chunk) {
        this.chunk = chunk;
    }

    public static List<ChunkWrapper> getWrappers(List<Chunk> chunks) {
        ArrayList<ChunkWrapper> wrappers = new ArrayList<ChunkWrapper>();
        chunks.forEach(c -> wrappers.add(new ChunkWrapper(c)));
        return wrappers;
    }

    public ChunkWrapper copy(long addedTime) {
        return new ChunkWrapper(this.chunk.copy(addedTime));
    }

    public Chunk getChunk() {
        return this.chunk;
    }

    public byte[] serialise(DataRepresentation state, Optional<SecretKey> secretKey) {
        if (this.chunk != null) {
            return this.chunk.serialise(state, secretKey);
        }
        
        return new byte[1];
    }

    public String getPrimaryAttribute() {
        if (this.chunk != null) {
            this.chunk.getPrimaryAttribute();
        }
        return "fake";
    }
    
    public BigInteger getSum() {
        if (this.chunk != null) {
            return this.chunk.getSum();
        }
        return BigInteger.valueOf(1000);
    }

    public long getFirstEntryTimestamp() {
        if (this.chunk != null) {
            return this.chunk.getFirstEntry().getTimestamp();
        }
        return firstEntryTimestamp;
    }

    public long getLastEntryTimestamp() {
        if (this.chunk != null) {
            return this.chunk.getLastEntry().getTimestamp();
        }
        return lastEntryTimestamp;
    }
}