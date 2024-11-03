package io.github.elimelt.pmqueue;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

public class PersistentMessageQueue implements Closeable {
    private static final boolean DEBUG = false;
    // size params
    private static final int QUEUE_HEADER_SIZE = 24;
    private static final int BLOCK_HEADER_SIZE = 8;
    private static final long MAX_FILE_SIZE = 1024L * 1024L * 1024L;
    private static final int INITIAL_FILE_SIZE = QUEUE_HEADER_SIZE;
    private static final int PAGE_SIZE = 4096;
    private static final int DEFAULT_BUFFER_SIZE = (1024 * 1024 / PAGE_SIZE) * PAGE_SIZE; // 1MB aligned
    private static final int MAX_BUFFER_SIZE = (8 * 1024 * 1024 / PAGE_SIZE) * PAGE_SIZE; // 8MB aligned

    // write batching
    private static final int BATCH_THRESHOLD = 64;
    private final ByteBuffer writeBatchBuffer;
    private int batchSize = 0;
    private long batchStartOffset;

    // store
    private final FileChannel channel;
    private final RandomAccessFile file;
    private ByteBuffer messageBuffer;

    // offsets
    private volatile long frontOffset;
    private volatile long rearOffset;

    // utils
    private final ReentrantLock lock;
    private final CRC32 checksumCalculator;

    public PersistentMessageQueue(String filename) throws IOException {
        File f = new File(filename);
        boolean isNew = !f.exists();
        this.file = new RandomAccessFile(f, "rw");
        this.channel = file.getChannel();

        // Use direct buffers for better I/O performance
        this.messageBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
        this.writeBatchBuffer = ByteBuffer.allocateDirect(MAX_BUFFER_SIZE);

        this.lock = new ReentrantLock(true); // Fair lock for predictable ordering
        this.checksumCalculator = new CRC32();

        if (isNew) {
            initializeNewFile();
        } else {
            loadMetadata();
        }
    }

    public boolean offer(Message message) throws IOException {
        if (message == null)
            throw new NullPointerException("Message cannot be null");

        byte[] serialized = MessageSerializer.serialize(message);
        int totalSize = BLOCK_HEADER_SIZE + serialized.length;

        lock.lock();
        try {
            if (rearOffset + totalSize > MAX_FILE_SIZE)
                return false;

            // Ensure file has enough space
            long requiredLength = rearOffset + totalSize;
            if (requiredLength > file.length()) {
                long newSize = Math.min(MAX_FILE_SIZE,
                        Math.max(file.length() * 2, requiredLength + DEFAULT_BUFFER_SIZE));
                file.setLength(newSize);
            }

            checksumCalculator.reset();
            checksumCalculator.update(serialized);
            int checksum = (int) checksumCalculator.getValue();

            // Start new batch if needed
            if (batchSize == 0) {
                batchStartOffset = rearOffset;
            }

            // Add to batch if message is small enough
            if (serialized.length < DEFAULT_BUFFER_SIZE / 4 &&
                totalSize <= MAX_BUFFER_SIZE - writeBatchBuffer.position() &&
                batchSize < BATCH_THRESHOLD) {

                writeBatchBuffer.putInt(serialized.length);
                writeBatchBuffer.putInt(checksum);
                writeBatchBuffer.put(serialized);
                batchSize++;

                rearOffset += totalSize;

                // Flush if batch is full
                if (batchSize >= BATCH_THRESHOLD ||
                    writeBatchBuffer.position() >= writeBatchBuffer.capacity() / 2) {
                    flushBatch();
                }
            } else {
                // Flush any existing batch first
                if (batchSize > 0) {
                    flushBatch();
                }

                // Handle large message directly
                if (serialized.length + BLOCK_HEADER_SIZE > messageBuffer.capacity()) {
                    messageBuffer = ByteBuffer.allocateDirect(
                            Math.min(MAX_BUFFER_SIZE, serialized.length + BLOCK_HEADER_SIZE));
                }

                messageBuffer.clear();
                messageBuffer.putInt(serialized.length);
                messageBuffer.putInt(checksum);
                messageBuffer.put(serialized);
                messageBuffer.flip();

                channel.write(messageBuffer, rearOffset);
                rearOffset += totalSize;
                saveMetadata();
            }

            return true;
        } finally {
            lock.unlock();
        }
    }

    private void flushBatch() throws IOException {
        if (batchSize > 0) {
            writeBatchBuffer.flip();
            channel.write(writeBatchBuffer, batchStartOffset);
            writeBatchBuffer.clear();
            batchSize = 0;
            saveMetadata();
        }
    }

    public Message poll() throws IOException {
        lock.lock();
        try {
            if (isEmpty())
                return null;

            // Flush any pending writes before reading
            if (batchSize > 0) {
                flushBatch();
            }

            // Read message header
            ByteBuffer headerBuffer = ByteBuffer.allocate(BLOCK_HEADER_SIZE);
            int bytesRead = channel.read(headerBuffer, frontOffset);
            if (bytesRead != BLOCK_HEADER_SIZE) {
                throw new IOException("Failed to read message header");
            }
            headerBuffer.flip();

            int messageSize = headerBuffer.getInt();
            int storedChecksum = headerBuffer.getInt();

            if (messageSize <= 0 || frontOffset + BLOCK_HEADER_SIZE + messageSize > file.length()) {
                throw new IOException(String.format(
                    "Corrupted queue: invalid block size %d at offset %d (file length: %d)",
                    messageSize, frontOffset, file.length()));
            }

            // Read message data
            ByteBuffer dataBuffer = ByteBuffer.allocate(messageSize);
            bytesRead = channel.read(dataBuffer, frontOffset + BLOCK_HEADER_SIZE);
            if (bytesRead != messageSize) {
                throw new IOException("Failed to read message data");
            }
            dataBuffer.flip();

            byte[] data = new byte[messageSize];
            dataBuffer.get(data);

            checksumCalculator.reset();
            checksumCalculator.update(data);
            int calculatedChecksum = (int) checksumCalculator.getValue();

            if (storedChecksum != calculatedChecksum) {
                throw new IOException(String.format(
                    "Corrupted message: checksum mismatch at offset %d. Expected: %d, Got: %d",
                    frontOffset, storedChecksum, calculatedChecksum));
            }

            Message message = MessageSerializer.deserialize(data);
            frontOffset += BLOCK_HEADER_SIZE + messageSize;
            saveMetadata();

            return message;
        } finally {
            lock.unlock();
        }
    }

    private void loadMetadata() throws IOException {
        if (file.length() < QUEUE_HEADER_SIZE) {
            throw new IOException("File too small to contain valid header");
        }

        ByteBuffer buffer = ByteBuffer.allocate(QUEUE_HEADER_SIZE);
        int bytesRead = channel.read(buffer, 0);
        if (bytesRead != QUEUE_HEADER_SIZE) {
            throw new IOException("Failed to read queue metadata");
        }
        buffer.flip();

        frontOffset = buffer.getLong();
        rearOffset = buffer.getLong();

        if (frontOffset < QUEUE_HEADER_SIZE || rearOffset < QUEUE_HEADER_SIZE ||
                frontOffset > file.length() || rearOffset > file.length() ||
                frontOffset > rearOffset) {
            throw new IOException("Corrupted queue metadata");
        }
    }

    private void saveMetadata() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(QUEUE_HEADER_SIZE);
        buffer.putLong(frontOffset);
        buffer.putLong(rearOffset);
        buffer.flip();
        channel.write(buffer, 0);
        channel.force(true);
    }

    public boolean isEmpty() {
        return frontOffset >= rearOffset && batchSize == 0;
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            flushBatch();
            saveMetadata();
            channel.force(true);
            channel.close();
            file.close();
        } finally {
            lock.unlock();
        }
    }

    private void initializeNewFile() throws IOException {
        file.setLength(INITIAL_FILE_SIZE);
        frontOffset = QUEUE_HEADER_SIZE;
        rearOffset = QUEUE_HEADER_SIZE;
        saveMetadata();
    }

    private void debug(String format, Object... args) {
        if (DEBUG) {
            System.out.printf("[DEBUG] " + format + "%n", args);
        }
    }
}