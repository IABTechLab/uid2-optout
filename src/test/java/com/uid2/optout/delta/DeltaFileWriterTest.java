package com.uid2.optout.delta;

import com.uid2.shared.optout.OptOutConst;
import com.uid2.shared.optout.OptOutEntry;
import com.uid2.shared.optout.OptOutUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class DeltaFileWriterTest {

    // Test constants
    private static final long WINDOW_START = 1700000000L;
    private static final long WINDOW_END = 1700003600L;
    private static final long ENTRY_TIMESTAMP = 1700001000L;

    private DeltaFileWriter writer;
    private ByteArrayOutputStream outputStream;

    @BeforeEach
    void setUp() {
        writer = new DeltaFileWriter(OptOutConst.EntrySize);
        outputStream = new ByteArrayOutputStream();
    }

    // ==================== writeStartOfDelta tests ====================

    @Test
    void testWriteStartOfDelta_writesCorrectSize() throws IOException {
        writer.writeStartOfDelta(outputStream, WINDOW_START);

        assertEquals(OptOutConst.EntrySize, outputStream.size());
    }

    @Test
    void testWriteStartOfDelta_writesNullHashBytes() throws IOException {
        writer.writeStartOfDelta(outputStream, WINDOW_START);

        byte[] result = outputStream.toByteArray();
        assertArrayEquals(OptOutUtils.nullHashBytes, extractFirstHash(result));
        assertArrayEquals(OptOutUtils.nullHashBytes, extractSecondHash(result));
    }

    @Test
    void testWriteStartOfDelta_writesTimestamp() throws IOException {
        writer.writeStartOfDelta(outputStream, WINDOW_START);

        assertEquals(WINDOW_START, extractTimestamp(outputStream.toByteArray()));
    }

    // ==================== writeEndOfDelta tests ====================

    @Test
    void testWriteEndOfDelta_writesCorrectSize() throws IOException {
        writer.writeEndOfDelta(outputStream, WINDOW_END);

        assertEquals(OptOutConst.EntrySize, outputStream.size());
    }

    @Test
    void testWriteEndOfDelta_writesOnesHashBytes() throws IOException {
        writer.writeEndOfDelta(outputStream, WINDOW_END);

        byte[] result = outputStream.toByteArray();
        assertArrayEquals(OptOutUtils.onesHashBytes, extractFirstHash(result));
        assertArrayEquals(OptOutUtils.onesHashBytes, extractSecondHash(result));
    }

    @Test
    void testWriteEndOfDelta_writesTimestamp() throws IOException {
        writer.writeEndOfDelta(outputStream, WINDOW_END);

        assertEquals(WINDOW_END, extractTimestamp(outputStream.toByteArray()));
    }

    // ==================== writeOptOutEntry tests ====================

    @Test
    void testWriteOptOutEntry_writesCorrectSize() throws IOException {
        writeTestOptOutEntry((byte) 0xAA, (byte) 0xBB, ENTRY_TIMESTAMP);

        assertEquals(OptOutConst.EntrySize, outputStream.size());
    }

    @Test
    void testWriteOptOutEntry_writesHashBytes() throws IOException {
        byte[] hashBytes = createTestBytes(32, (byte) 0xAA);
        writeTestOptOutEntry((byte) 0xAA, (byte) 0xBB, ENTRY_TIMESTAMP);

        assertArrayEquals(hashBytes, extractFirstHash(outputStream.toByteArray()));
    }

    @Test
    void testWriteOptOutEntry_writesIdBytes() throws IOException {
        byte[] idBytes = createTestBytes(32, (byte) 0xBB);
        writeTestOptOutEntry((byte) 0xAA, (byte) 0xBB, ENTRY_TIMESTAMP);

        assertArrayEquals(idBytes, extractSecondHash(outputStream.toByteArray()));
    }

    @Test
    void testWriteOptOutEntry_canBeReadByOptOutEntry() throws IOException {
        byte[] hashBytes = createTestBytes(32, (byte) 0x11);
        byte[] idBytes = createTestBytes(32, (byte) 0x22);
        long timestamp = 1700001234L;

        writer.writeOptOutEntry(outputStream, hashBytes, idBytes, timestamp);

        OptOutEntry entry = OptOutEntry.parse(outputStream.toByteArray(), 0);
        assertArrayEquals(hashBytes, entry.identityHash);
        assertArrayEquals(idBytes, entry.advertisingId);
        assertEquals(timestamp, entry.timestamp);
    }

    // ==================== Multiple entries tests ====================

    @Test
    void testWriteMultipleEntries_writesCorrectTotalSize() throws IOException {
        writeCompleteDeltaFile(2);

        // 4 entries (start + 2 opt-out + end) * 72 bytes = 288 bytes
        assertEquals(4 * OptOutConst.EntrySize, outputStream.size());
    }

    @Test
    void testWriteMultipleEntries_entriesAreContiguous() throws IOException {
        byte[] hashBytes = createTestBytes(32, (byte) 0xEE);
        
        writer.writeStartOfDelta(outputStream, WINDOW_START);
        writeTestOptOutEntry((byte) 0xEE, (byte) 0xFF, WINDOW_START + 500);
        writer.writeEndOfDelta(outputStream, WINDOW_END);

        byte[] result = outputStream.toByteArray();
        
        // Verify start entry - null hashes at offset 0
        assertArrayEquals(OptOutUtils.nullHashBytes, extractHashAtOffset(result, 0));
        
        // Verify opt-out entry - custom hash at offset 72
        assertArrayEquals(hashBytes, extractHashAtOffset(result, OptOutConst.EntrySize));
        
        // Verify end entry - ones hashes at offset 144
        assertArrayEquals(OptOutUtils.onesHashBytes, extractHashAtOffset(result, 2 * OptOutConst.EntrySize));
    }

    // ==================== Buffer capacity tests ====================

    @Test
    void testSmallBufferSize_expandsAutomatically() throws IOException {
        DeltaFileWriter smallBufferWriter = new DeltaFileWriter(16);

        assertDoesNotThrow(() -> smallBufferWriter.writeStartOfDelta(outputStream, WINDOW_START));
        assertEquals(OptOutConst.EntrySize, outputStream.size());
    }

    @Test
    void testSmallBuffer_multipleWrites() throws IOException {
        DeltaFileWriter smallBufferWriter = new DeltaFileWriter(8);
        
        smallBufferWriter.writeStartOfDelta(outputStream, WINDOW_START);
        writeTestOptOutEntry(smallBufferWriter, (byte) 0x12, (byte) 0x34, ENTRY_TIMESTAMP);
        smallBufferWriter.writeEndOfDelta(outputStream, WINDOW_END);

        assertEquals(3 * OptOutConst.EntrySize, outputStream.size());
    }

    @Test
    void testLargeBufferSize_worksCorrectly() throws IOException {
        DeltaFileWriter largeBufferWriter = new DeltaFileWriter(1024 * 1024);

        largeBufferWriter.writeStartOfDelta(outputStream, WINDOW_START);

        assertEquals(OptOutConst.EntrySize, outputStream.size());
    }

    // ==================== Edge cases ====================

    @Test
    void testZeroTimestamp() throws IOException {
        writer.writeStartOfDelta(outputStream, 0L);

        assertEquals(0L, extractTimestamp(outputStream.toByteArray()));
    }

    @Test
    void testMaxTimestamp() throws IOException {
        writer.writeStartOfDelta(outputStream, Long.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, extractTimestamp(outputStream.toByteArray()));
    }

    @Test
    void testReuseWriterForMultipleFiles() throws IOException {
        ByteArrayOutputStream stream1 = new ByteArrayOutputStream();
        ByteArrayOutputStream stream2 = new ByteArrayOutputStream();
        
        // Write first file
        writer.writeStartOfDelta(stream1, WINDOW_START);
        writer.writeEndOfDelta(stream1, WINDOW_END);
        
        // Write second file with same writer
        writer.writeStartOfDelta(stream2, WINDOW_START + 10000);
        writer.writeEndOfDelta(stream2, WINDOW_END + 10000);

        assertEquals(2 * OptOutConst.EntrySize, stream1.size());
        assertEquals(2 * OptOutConst.EntrySize, stream2.size());
        assertNotEquals(extractTimestamp(stream1.toByteArray()), extractTimestamp(stream2.toByteArray()));
    }

    // ==================== Helper methods ====================

    private byte[] createTestBytes(int size, byte fillValue) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, fillValue);
        return bytes;
    }

    private void writeTestOptOutEntry(byte hashFill, byte idFill, long timestamp) throws IOException {
        writeTestOptOutEntry(writer, hashFill, idFill, timestamp);
    }

    private void writeTestOptOutEntry(DeltaFileWriter w, byte hashFill, byte idFill, long timestamp) throws IOException {
        byte[] hashBytes = createTestBytes(32, hashFill);
        byte[] idBytes = createTestBytes(32, idFill);
        w.writeOptOutEntry(outputStream, hashBytes, idBytes, timestamp);
    }

    private void writeCompleteDeltaFile(int numOptOutEntries) throws IOException {
        writer.writeStartOfDelta(outputStream, WINDOW_START);
        for (int i = 0; i < numOptOutEntries; i++) {
            writeTestOptOutEntry((byte) (0xCC + i), (byte) (0xDD + i), ENTRY_TIMESTAMP + i * 100);
        }
        writer.writeEndOfDelta(outputStream, WINDOW_END);
    }

    private static byte[] extractFirstHash(byte[] data) {
        return Arrays.copyOfRange(data, 0, 32);
    }

    private static byte[] extractSecondHash(byte[] data) {
        return Arrays.copyOfRange(data, 32, 64);
    }

    private static byte[] extractHashAtOffset(byte[] data, int offset) {
        return Arrays.copyOfRange(data, offset, offset + 32);
    }

    private static long extractTimestamp(byte[] data) {
        return ByteBuffer.wrap(data, 64, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }
}
