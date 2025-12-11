package com.uid2.optout.delta;

import com.uid2.shared.optout.OptOutConst;
import com.uid2.shared.optout.OptOutEntry;
import com.uid2.shared.optout.OptOutUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Handles binary writing of delta file entries.
 * 
 * Delta files have the following format:
 * - Start entry: null hash (32 bytes) + null hash (32 bytes) + timestamp (8 bytes)
 * - Opt-out entries: hash (32 bytes) + id (32 bytes) + timestamp (7 bytes) + metadata (1 byte)
 * - End entry: ones hash (32 bytes) + ones hash (32 bytes) + timestamp (8 bytes)
 * 
 * Each entry is 72 bytes (OptOutConst.EntrySize)
 */
public class DeltaFileWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeltaFileWriter.class);
    
    private ByteBuffer buffer;
    
    /**
     * Create a DeltaFileWriter with the specified initial buffer size.
     * 
     * @param bufferSize Initial buffer size in bytes
     */
    public DeltaFileWriter(int bufferSize) {
        this.buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
    }
    
    /**
     * Write the start-of-delta sentinel entry.
     * Uses null hash bytes and the window start timestamp.
     * 
     * @param stream Output stream to write to
     * @param windowStart Window start timestamp (epoch seconds)
     * @throws IOException if write fails
     */
    public void writeStartOfDelta(ByteArrayOutputStream stream, long windowStart) throws IOException {
        ensureCapacity(OptOutConst.EntrySize);
        
        buffer.put(OptOutUtils.nullHashBytes);
        buffer.put(OptOutUtils.nullHashBytes);
        buffer.putLong(windowStart);
        
        flushToStream(stream);
    }
    
    /**
     * Write a single opt-out entry.
     * 
     * @param stream Output stream to write to
     * @param hashBytes Hash bytes (32 bytes)
     * @param idBytes ID bytes (32 bytes)
     * @param timestamp Entry timestamp (epoch seconds)
     * @throws IOException if write fails
     */
    public void writeOptOutEntry(ByteArrayOutputStream stream, byte[] hashBytes, byte[] idBytes, long timestamp) throws IOException {
        ensureCapacity(OptOutConst.EntrySize);
        
        OptOutEntry.writeTo(buffer, hashBytes, idBytes, timestamp);
        
        flushToStream(stream);
    }
    
    /**
     * Write the end-of-delta sentinel entry.
     * Uses ones hash bytes and the window end timestamp.
     * 
     * @param stream Output stream to write to
     * @param windowEnd Window end timestamp (epoch seconds)
     * @throws IOException if write fails
     */
    public void writeEndOfDelta(ByteArrayOutputStream stream, long windowEnd) throws IOException {
        ensureCapacity(OptOutConst.EntrySize);
        
        buffer.put(OptOutUtils.onesHashBytes);
        buffer.put(OptOutUtils.onesHashBytes);
        buffer.putLong(windowEnd);
        
        flushToStream(stream);
    }
    
    /**
     * Flush the buffer contents to the output stream and clear the buffer.
     */
    private void flushToStream(ByteArrayOutputStream stream) throws IOException {
        buffer.flip();
        byte[] entry = new byte[buffer.remaining()];
        buffer.get(entry);
        stream.write(entry);
        buffer.clear();
    }
    
    /**
     * Ensure buffer has sufficient capacity, expanding if necessary.
     */
    private void ensureCapacity(int dataSize) {
        if (buffer.capacity() < dataSize) {
            int newCapacity = Integer.highestOneBit(dataSize) << 1;
            LOGGER.info("expanding buffer size: current {}, need {}, new {}", buffer.capacity(), dataSize, newCapacity);
            this.buffer = ByteBuffer.allocate(newCapacity).order(ByteOrder.LITTLE_ENDIAN);
        }
    }
}

