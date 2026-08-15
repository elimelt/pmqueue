package io.github.elimelt.pmqueue.message;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Characterization tests that pin the exact wire format produced by
 * {@link MessageSerializer#serialize(Message)}.
 *
 * <p>
 * Current layout (16-byte header + data). All multi-byte fields are written
 * in the JVM's native byte order, because {@link MessageSerializer} writes
 * them with {@code sun.misc.Unsafe}:
 *
 * <pre>
 * offset 0  (8 bytes): timestamp (long)
 * offset 8  (4 bytes): message type (int)
 * offset 12 (4 bytes): data length (int)
 * offset 16 (n bytes): message data
 * </pre>
 *
 * <p>
 * These tests exist so later refactors of {@link MessageSerializer} cannot
 * silently change this on-wire layout; they must keep passing byte-for-byte
 * against unmodified source.
 */
class WireFormatTest {

  private static final int HEADER_SIZE = 16;
  private static final int TIMESTAMP_OFFSET = 0;
  private static final int TYPE_OFFSET = 8;
  private static final int LENGTH_OFFSET = 12;
  private static final int DATA_OFFSET = 16;

  // MessageSerializer writes header fields via Unsafe using native byte
  // order. On the x86-64/ARM64 hosts this project runs on, that is
  // little-endian; pin that assumption explicitly so a change of host
  // architecture (rather than a code change) is what would break this test.
  private static final ByteOrder WIRE_ORDER = ByteOrder.LITTLE_ENDIAN;

  @Test
  @DisplayName("Precondition: host native byte order is little-endian")
  void nativeOrderIsLittleEndian() {
    assertEquals(ByteOrder.LITTLE_ENDIAN, ByteOrder.nativeOrder(),
        "MessageSerializer relies on Unsafe's native-order writes; "
            + "these fixed-offset assertions assume a little-endian host");
  }

  @Test
  @DisplayName("Header fields sit at fixed offsets: type@8, length@12, data@16")
  void headerFieldsAtFixedOffsets() throws IOException {
    byte[] data = { 0x41, 0x42, 0x43, 0x44 }; // "ABCD"
    int type = 0x12345678;
    Message message = new Message(data, type);

    byte[] wire = MessageSerializer.serialize(message);

    assertEquals(HEADER_SIZE + data.length, wire.length,
        "Wire size must be header (16) + data length");

    ByteBuffer buf = ByteBuffer.wrap(wire).order(WIRE_ORDER);

    assertEquals(message.getTimestamp(), buf.getLong(TIMESTAMP_OFFSET),
        "Timestamp bytes at offset 0 must decode to the message's timestamp");
    assertEquals(type, buf.getInt(TYPE_OFFSET), "Message type must be at offset 8");
    assertEquals(data.length, buf.getInt(LENGTH_OFFSET), "Data length must be at offset 12");

    byte[] decodedData = new byte[data.length];
    buf.position(DATA_OFFSET);
    buf.get(decodedData);
    assertArrayEquals(data, decodedData, "Data must start at offset 16");
  }

  @Test
  @DisplayName("Empty data produces exactly a 16-byte header-only wire encoding")
  void emptyDataProducesHeaderOnlyWire() throws IOException {
    Message message = new Message(new byte[0], 7);

    byte[] wire = MessageSerializer.serialize(message);

    assertEquals(HEADER_SIZE, wire.length);
    ByteBuffer buf = ByteBuffer.wrap(wire).order(WIRE_ORDER);
    assertEquals(7, buf.getInt(TYPE_OFFSET));
    assertEquals(0, buf.getInt(LENGTH_OFFSET));
  }

  @Test
  @DisplayName("Message type is written as raw 4-byte little-endian bits, including negative values")
  void negativeMessageTypeBitsPreserved() throws IOException {
    Message message = new Message(new byte[] { 9 }, -1);

    byte[] wire = MessageSerializer.serialize(message);

    assertArrayEquals(
        new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF },
        new byte[] { wire[8], wire[9], wire[10], wire[11] },
        "type == -1 must serialize as four 0xFF bytes at offset 8");
  }

  @Test
  @DisplayName("Timestamp occupies bytes 0-7; masking them leaves a fully deterministic remainder")
  void timestampBytesPositionAndPlausibility() throws IOException {
    long before = System.currentTimeMillis();
    Message message = new Message(new byte[] { 'x' }, 1);
    long after = System.currentTimeMillis();

    byte[] wire = MessageSerializer.serialize(message);
    ByteBuffer buf = ByteBuffer.wrap(wire).order(WIRE_ORDER);
    long timestampFromWire = buf.getLong(TIMESTAMP_OFFSET);

    assertTrue(timestampFromWire >= before && timestampFromWire <= after,
        "Timestamp bytes at offset 0 must decode to a plausible current-time-millis value");
    assertEquals(message.getTimestamp(), timestampFromWire);

    byte[] maskedWire = wire.clone();
    for (int i = TIMESTAMP_OFFSET; i < TYPE_OFFSET; i++) {
      maskedWire[i] = 0;
    }

    byte[] expectedMasked = {
        0, 0, 0, 0, 0, 0, 0, 0, // masked timestamp (offset 0-7)
        1, 0, 0, 0, // type = 1, little-endian (offset 8-11)
        1, 0, 0, 0, // length = 1, little-endian (offset 12-15)
        'x' // data (offset 16)
    };
    assertArrayEquals(expectedMasked, maskedWire,
        "With the timestamp masked, the rest of the wire encoding is fully deterministic");
  }

  @Test
  @DisplayName("deserialize() reads timestamp/type/length/data from the documented fixed offsets")
  void deserializeReadsFixedOffsets() throws IOException {
    byte[] data = "payload".getBytes();
    int type = 42;
    long timestamp = 1_700_000_000_000L;

    ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + data.length).order(WIRE_ORDER);
    buf.putLong(TIMESTAMP_OFFSET, timestamp);
    buf.putInt(TYPE_OFFSET, type);
    buf.putInt(LENGTH_OFFSET, data.length);
    buf.position(DATA_OFFSET);
    buf.put(data);

    Message decoded = MessageSerializer.deserialize(buf.array());

    assertEquals(timestamp, decoded.getTimestamp());
    assertEquals(type, decoded.getMessageType());
    assertArrayEquals(data, decoded.getData());
  }
}
