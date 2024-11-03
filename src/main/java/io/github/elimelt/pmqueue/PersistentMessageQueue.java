package io.github.elimelt.pmqueue;;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

public class PersistentMessageQueue implements Closeable {
    private static final boolean DEBUG = false;
    // size params
    private static final int QUEUE_HEADER_SIZE = 24;
    private static final int BLOCK_HEADER_SIZE = 8;
    private static final long MAX_FILE_SIZE = 1024L * 1024L * 1024L;
    private static final int INITIAL_FILE_SIZE = QUEUE_HEADER_SIZE;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1MB
    private static final int MAX_BUFFER_SIZE = 8 * 1024 * 1024; // 8MB

    // write batching
    private static final int BATCH_THRESHOLD = 64;
    private final ByteBuffer writeBatchBuffer;
    private int batchSize = 0;

    // RA buffer
    private final ByteBuffer readAheadBuffer;
    private long readAheadPosition = -1;
    private int readAheadLimit = 0;

    // store
    private final FileChannel channel;
    private final RandomAccessFile file;
    private ByteBuffer messageBuffer;

    // offsets
    private long frontOffset;
    private long rearOffset;
    private long batchStartOffset;
    @SuppressWarnings("unused")
    private long currentBatchSize;

    // utils
    private final ReentrantLock lock;
    private final CRC32 checksumCalculator;

    public PersistentMessageQueue(String filename) throws IOException {
        File f = new File(filename);
        boolean isNew = !f.exists();
        this.file = new RandomAccessFile(f, "rw");
        this.channel = file.getChannel();

        // Enable OS-level optimizations
        channel.force(true);

        // Use direct buffers for better I/O performance
        this.messageBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
        this.writeBatchBuffer = ByteBuffer.allocateDirect(MAX_BUFFER_SIZE);
        this.readAheadBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);

        this.lock = new ReentrantLock(false); // Using unfair lock for better throughput
        this.checksumCalculator = new CRC32();

        if (isNew) {
            initializeNewFile();
        } else {
            loadMetadata();
        }
    }

    private void debug(String format, Object... args) {
        if (DEBUG) {
            System.out.printf("[DEBUG] " + format + "%n", args);
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

            // Calculate checksum
            checksumCalculator.reset();
            checksumCalculator.update(serialized);
            int checksum = (int) checksumCalculator.getValue();

            debug("Writing message: size=%d, checksum=%d, offset=%d",
                    serialized.length, checksum, rearOffset);

            // Start new batch if needed
            if (batchSize == 0) {
                batchStartOffset = rearOffset;
                currentBatchSize = 0;
            }

            // Add to batch buffer if message is small enough
            if (totalSize <= MAX_BUFFER_SIZE - writeBatchBuffer.position() &&
                    batchSize < BATCH_THRESHOLD &&
                    serialized.length < DEFAULT_BUFFER_SIZE / 4) { // Only batch relatively small messages

                writeBatchBuffer.putInt(serialized.length);
                writeBatchBuffer.putInt(checksum);
                writeBatchBuffer.put(serialized);
                batchSize++;
                currentBatchSize += totalSize;

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

                // Write large message directly
                if (serialized.length > messageBuffer.capacity()) {
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

            debug("Flushing batch: size=%d, position=%d, offset=%d",
                    batchSize, writeBatchBuffer.position(), batchStartOffset);

            long bytesWritten = channel.write(writeBatchBuffer, batchStartOffset);

            debug("Batch written: bytes=%d", bytesWritten);

            // Update rear offset based on actual bytes written
            rearOffset = batchStartOffset + bytesWritten;

            writeBatchBuffer.clear();
            batchSize = 0;
            currentBatchSize = 0;

            // Force metadata update after batch
            saveMetadata();
            // Force write to disk for batches
            channel.force(false);
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

            // Check if we need to read ahead
            if (frontOffset != readAheadPosition ||
                    frontOffset >= readAheadPosition + readAheadLimit) {
                readAhead();
            }

            // Read from read-ahead buffer
            int bufferPosition = (int) (frontOffset - readAheadPosition);
            if (bufferPosition >= readAheadBuffer.limit()) {
                readAhead();
                bufferPosition = 0;
            }

            readAheadBuffer.position(bufferPosition);

            int blockSize = readAheadBuffer.getInt();
            int storedChecksum = readAheadBuffer.getInt();

            debug("Reading message: size=%d, checksum=%d, offset=%d",
                    blockSize, storedChecksum, frontOffset);

            if (blockSize <= 0 || frontOffset + BLOCK_HEADER_SIZE + blockSize > file.length()) {
                throw new IOException(String.format(
                        "Corrupted queue: invalid block size %d at offset %d (file length: %d)",
                        blockSize, frontOffset, file.length()));
            }

            // If message would extend beyond read-ahead buffer, do direct read
            if (bufferPosition + BLOCK_HEADER_SIZE + blockSize > readAheadBuffer.limit()) {
                readDirect(blockSize, storedChecksum);
                return null; // Retry on next poll()
            }

            byte[] data = new byte[blockSize];
            readAheadBuffer.get(data);

            checksumCalculator.reset();
            checksumCalculator.update(data);
            int calculatedChecksum = (int) checksumCalculator.getValue();

            if (storedChecksum != calculatedChecksum) {
                throw new IOException(String.format(
                        "Corrupted message: checksum mismatch at offset %d. Expected: %d, Got: %d",
                        frontOffset, storedChecksum, calculatedChecksum));
            }

            Message message = MessageSerializer.deserialize(data);

            frontOffset += BLOCK_HEADER_SIZE + blockSize;

            // Save metadata periodically
            if (frontOffset % (DEFAULT_BUFFER_SIZE) == 0) {
                saveMetadata();
            }

            return message;
        } finally {
            lock.unlock();
        }
    }

    private void readDirect(int blockSize, int storedChecksum) throws IOException {
        // Allocate temporary buffer for direct read
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(blockSize);
        channel.read(directBuffer, frontOffset + BLOCK_HEADER_SIZE);
        directBuffer.flip();

        byte[] data = new byte[blockSize];
        directBuffer.get(data);

        checksumCalculator.reset();
        checksumCalculator.update(data);
        int calculatedChecksum = (int) checksumCalculator.getValue();

        if (storedChecksum != calculatedChecksum) {
            throw new IOException(String.format(
                    "Corrupted message: checksum mismatch in direct read at offset %d",
                    frontOffset));
        }

        frontOffset += BLOCK_HEADER_SIZE + blockSize;
        saveMetadata();
    }

    private void readAhead() throws IOException {
        readAheadBuffer.clear();
        readAheadPosition = frontOffset;
        readAheadLimit = (int) Math.min(readAheadBuffer.capacity(), rearOffset - frontOffset);

        debug("Reading ahead: position=%d, limit=%d", readAheadPosition, readAheadLimit);

        readAheadBuffer.limit(readAheadLimit);
        int bytesRead = channel.read(readAheadBuffer, readAheadPosition);

        debug("Read ahead complete: bytes=%d", bytesRead);

        readAheadBuffer.flip();
    }

    @SuppressWarnings("unused")
    private void reset() throws IOException {
        frontOffset = QUEUE_HEADER_SIZE;
        rearOffset = QUEUE_HEADER_SIZE;
        file.setLength(INITIAL_FILE_SIZE);
        saveMetadata();
    }

    @SuppressWarnings("unused")
    private void compact() throws IOException {
        if (frontOffset <= QUEUE_HEADER_SIZE || isEmpty())
            return;

        long dataSize = rearOffset - frontOffset;
        if (dataSize <= 0)
            return;

        // Use a larger buffer for compaction
        ByteBuffer compactBuffer = ByteBuffer.allocateDirect(Math.min((int) dataSize, 1024 * 1024));
        long currentPos = frontOffset;
        long writePos = QUEUE_HEADER_SIZE;

        while (currentPos < rearOffset) {
            compactBuffer.clear();
            int bytesToRead = (int) Math.min(compactBuffer.capacity(), rearOffset - currentPos);
            compactBuffer.limit(bytesToRead);

            channel.read(compactBuffer, currentPos);
            compactBuffer.flip();
            channel.write(compactBuffer, writePos);

            currentPos += bytesToRead;
            writePos += bytesToRead;
        }

        frontOffset = QUEUE_HEADER_SIZE;
        rearOffset = writePos;
        file.setLength(rearOffset);
        saveMetadata();
    }

    private void loadMetadata() throws IOException {
        if (file.length() < QUEUE_HEADER_SIZE) {
            throw new IOException("File too small to contain valid header");
        }

        ByteBuffer buffer = ByteBuffer.allocate(QUEUE_HEADER_SIZE);
        channel.read(buffer, 0);
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
        return frontOffset >= rearOffset;
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            // Ensure any batched writes are flushed
            flushBatch();
            saveMetadata();
            if (channel.isOpen()) {
                channel.force(true);
                channel.close();
            }
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

    public static void main(String[] args) throws IOException {
        // Enable JFR if specified
        if (args.length > 0 && args[0].equals("--jfr")) {
            try {
                Class.forName("jdk.jfr.Recording").getMethod("start").invoke(
                        Class.forName("jdk.jfr.Recording").getConstructor().newInstance());
            } catch (Exception e) {
                System.err.println("Could not start JFR recording: " + e);
            }
        }

        String filename = "queue_test_" + System.currentTimeMillis() + ".dat";
        File queueFile = new File(filename);

        try {
            // Warm up JVM
            System.out.println("Warming up JVM...");
            warmup(filename);

            System.out.println("\nStarting benchmarks...");
            // Run benchmarks with different message sizes
            int[] messageSizes = { 100, 1000, 10000, 100000 };
            int[] counts = { 10000, 5000, 1000, 100 }; // Adjusted counts for each size

            for (int i = 0; i < messageSizes.length; i++) {
                detailedBenchmark(filename, messageSizes[i], counts[i]);
            }

        } finally {
            queueFile.delete();
        }
    }

    private static void warmup(String filename) throws IOException {
        // Run a shorter version of the benchmark to warm up the JVM
        for (int i = 0; i < 3; i++) {
            benchmarkWithSize(filename, 1000, 100);
        }
    }

    private static void detailedBenchmark(String filename, int messageSize, int messageCount) throws IOException {
        System.out.printf("\nDetailed benchmark: size=%d bytes, count=%d%n", messageSize, messageCount);

        byte[] data = new byte[messageSize];
        ThreadLocalRandom.current().nextBytes(data);

        // Track detailed timings
        long writeStart, writeEnd, readStart, readEnd;
        long totalWriteTime = 0, totalReadTime = 0;
        long maxWriteTime = 0, maxReadTime = 0;
        long minWriteTime = Long.MAX_VALUE, minReadTime = Long.MAX_VALUE;

        try (PersistentMessageQueue queue = new PersistentMessageQueue(filename)) {
            // Write benchmark
            writeStart = System.nanoTime();

            for (int i = 0; i < messageCount; i++) {
                long msgWriteStart = System.nanoTime();
                queue.offer(new Message(data, i));
                long msgWriteTime = System.nanoTime() - msgWriteStart;

                totalWriteTime += msgWriteTime;
                maxWriteTime = Math.max(maxWriteTime, msgWriteTime);
                minWriteTime = Math.min(minWriteTime, msgWriteTime);

                if (i % (messageCount / 10) == 0) {
                    System.out.printf("Write progress: %d%%%n", (i * 100) / messageCount);
                }
            }
            writeEnd = System.nanoTime();

            // Read benchmark
            readStart = System.nanoTime();
            int readCount = 0;

            while (!queue.isEmpty()) {
                long msgReadStart = System.nanoTime();
                Message msg = queue.poll();
                long msgReadTime = System.nanoTime() - msgReadStart;

                if (msg != null) {
                    readCount++;
                    totalReadTime += msgReadTime;
                    maxReadTime = Math.max(maxReadTime, msgReadTime);
                    minReadTime = Math.min(minReadTime, msgReadTime);
                }

                if (readCount % (messageCount / 10) == 0) {
                    System.out.printf("Read progress: %d%%%n", (readCount * 100) / messageCount);
                }
            }
            readEnd = System.nanoTime();

            // Calculate and print detailed statistics
            double totalWriteSeconds = (writeEnd - writeStart) / 1_000_000_000.0;
            double totalReadSeconds = (readEnd - readStart) / 1_000_000_000.0;
            double avgWriteMs = (totalWriteTime / messageCount) / 1_000_000.0;
            double avgReadMs = (totalReadTime / readCount) / 1_000_000.0;

            System.out.println("\nWrite Statistics:");
            System.out.printf("Total time: %.2f seconds%n", totalWriteSeconds);
            System.out.printf("Throughput: %.2f MB/s%n",
                (messageSize * messageCount) / (totalWriteSeconds * 1024 * 1024));
            System.out.printf("Average latency: %.3f ms%n", avgWriteMs);
            System.out.printf("Min latency: %.3f ms%n", minWriteTime / 1_000_000.0);
            System.out.printf("Max latency: %.3f ms%n", maxWriteTime / 1_000_000.0);

            System.out.println("\nRead Statistics:");
            System.out.printf("Total time: %.2f seconds%n", totalReadSeconds);
            System.out.printf("Throughput: %.2f MB/s%n",
                (messageSize * readCount) / (totalReadSeconds * 1024 * 1024));
            System.out.printf("Average latency: %.3f ms%n", avgReadMs);
            System.out.printf("Min latency: %.3f ms%n", minReadTime / 1_000_000.0);
            System.out.printf("Max latency: %.3f ms%n", maxReadTime / 1_000_000.0);
        }
    }

    private static void benchmarkWithSize(String filename, int messageSize, int messageCount) throws IOException {
        System.out.printf("\nBenchmarking with message size: %d bytes, count: %d%n", messageSize, messageCount);

        byte[] data = new byte[messageSize];
        ThreadLocalRandom.current().nextBytes(data);

        try (PersistentMessageQueue queue = new PersistentMessageQueue(filename)) {
            // Write benchmark
            long startWrite = System.nanoTime();
            for (int i = 0; i < messageCount; i++) {
                queue.offer(new Message(data, i));
            }
            long writeTime = System.nanoTime() - startWrite;

            // Read benchmark
            long startRead = System.nanoTime();
            int readCount = 0;
            while (!queue.isEmpty()) {
                Message msg = queue.poll();
                if (msg != null)
                    readCount++;
            }
            long readTime = System.nanoTime() - startRead;

            System.out.printf("Write throughput: %.2f MB/s%n",
                    (messageSize * messageCount) / ((writeTime / 1_000_000_000.0) * 1024 * 1024));
            System.out.printf("Read throughput: %.2f MB/s%n",
                    (messageSize * readCount) / ((readTime / 1_000_000_000.0) * 1024 * 1024));
        }
    }

    private static void integrityTest(String filename) throws IOException {
        System.out.println("\nRunning integrity test...");
        try (PersistentMessageQueue queue = new PersistentMessageQueue(filename)) {
            // Write messages with known patterns
            for (int i = 0; i < 100; i++) {
                byte[] data = String.format("Test message %d", i).getBytes();
                queue.offer(new Message(data, i));
            }

            // Verify messages
            for (int i = 0; i < 100; i++) {
                Message msg = queue.poll();
                String content = new String(msg.getData());
                if (!content.equals(String.format("Test message %d", i))) {
                    throw new AssertionError("Message integrity test failed");
                }
            }
            System.out.println("Integrity test passed");
        }
    }

    private static void stressTest(String filename) throws IOException {
        System.out.println("\nRunning stress test...");
        try (PersistentMessageQueue queue = new PersistentMessageQueue(filename)) {
            // Alternate between writes and reads
            for (int i = 0; i < 1000; i++) {
                if (i % 2 == 0) {
                    byte[] data = new byte[ThreadLocalRandom.current().nextInt(100, 1000)];
                    ThreadLocalRandom.current().nextBytes(data);
                    queue.offer(new Message(data, i));
                } else {
                    queue.poll();
                }
            }
            System.out.println("Stress test passed");
        }
    }
}