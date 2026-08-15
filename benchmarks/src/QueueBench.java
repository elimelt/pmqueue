package bench;

import java.io.File;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import io.github.elimelt.pmqueue.MessageQueue;
import io.github.elimelt.pmqueue.QueueConfig;
import io.github.elimelt.pmqueue.core.PersistentMessageQueue;
import io.github.elimelt.pmqueue.message.Message;

/**
 * Plain-Java benchmark harness for the persistent message queue.
 *
 * One JVM invocation runs one scenario for one trial and prints a single
 * JSON line to stdout. The driver script (run_benchmarks.sh) handles trial
 * interleaving between the two library builds and collects the output.
 *
 * Only APIs that exist on both sides under comparison are used:
 * QueueConfig.Builder, new PersistentMessageQueue(QueueConfig),
 * MessageQueue, and Message.
 *
 * Scenarios:
 *   offer      timed offer loop (close included so buffered writes flush)
 *   poll       timed poll loop over a pre-populated file
 *   latency    single-message offer+poll round trips, percentiles reported
 *   openclose  open+close cycles on an existing populated file
 *
 * Memory metrics captured per run:
 *   - bytes allocated per operation (per-thread allocation counter delta)
 *   - GC count and GC time deltas over the measured window
 *   - steady-state heap after a full produce/consume cycle (poll scenario)
 */
public final class QueueBench {

  // prevents dead-code elimination of polled payloads
  private static long sink = 0;

  public static void main(String[] args) throws Exception {
    Map<String, String> a = new HashMap<>();
    for (String arg : args) {
      int eq = arg.indexOf('=');
      a.put(arg.substring(0, eq), arg.substring(eq + 1));
    }

    String scenario = a.get("scenario");
    String side = a.get("side");
    int trial = Integer.parseInt(a.get("trial"));
    int size = Integer.parseInt(a.get("size"));
    int ops = Integer.parseInt(a.get("ops"));
    int warmup = Integer.parseInt(a.get("warmup"));
    boolean checksum = Boolean.parseBoolean(a.get("checksum"));
    String dataDir = a.get("datadir");

    Result r;
    switch (scenario) {
      case "offer" -> r = benchOffer(dataDir, size, ops, warmup, checksum);
      case "poll" -> r = benchPoll(dataDir, size, ops, warmup, checksum);
      case "latency" -> r = benchLatency(dataDir, size, ops, warmup, checksum);
      case "openclose" -> r = benchOpenClose(dataDir, size, ops, warmup, checksum);
      default -> throw new IllegalArgumentException("unknown scenario: " + scenario);
    }

    StringBuilder json = new StringBuilder();
    json.append('{');
    json.append("\"scenario\":\"").append(scenario).append('"');
    json.append(",\"side\":\"").append(side).append('"');
    json.append(",\"trial\":").append(trial);
    json.append(",\"size\":").append(size);
    json.append(",\"checksum\":").append(checksum);
    json.append(",\"ops\":").append(ops);
    for (Map.Entry<String, Object> e : r.fields.entrySet()) {
      json.append(",\"").append(e.getKey()).append("\":").append(e.getValue());
    }
    json.append(",\"sink\":").append(sink % 1000);
    json.append('}');
    System.out.println(json);
  }

  private static final class Result {
    final Map<String, Object> fields = new java.util.LinkedHashMap<>();

    Result put(String k, Object v) {
      fields.put(k, v);
      return this;
    }
  }

  private static MessageQueue open(String path, boolean checksum) throws IOException {
    return new PersistentMessageQueue(new QueueConfig.Builder()
        .filePath(path)
        .checksumEnabled(checksum)
        .build());
  }

  private static byte[] payload(int size) {
    byte[] data = new byte[size];
    new Random(42).nextBytes(data);
    return data;
  }

  private static void deleteFile(String path) {
    new File(path).delete();
  }

  // --- memory helpers ----------------------------------------------------

  private static long threadAllocatedBytes() {
    com.sun.management.ThreadMXBean tb = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    return tb.getThreadAllocatedBytes(Thread.currentThread().threadId());
  }

  private static long[] gcSnapshot() {
    long count = 0;
    long timeMs = 0;
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      long c = gc.getCollectionCount();
      long t = gc.getCollectionTime();
      if (c > 0) {
        count += c;
      }
      if (t > 0) {
        timeMs += t;
      }
    }
    return new long[] { count, timeMs };
  }

  private static long settledHeapUsed() throws InterruptedException {
    for (int i = 0; i < 3; i++) {
      System.gc();
      Thread.sleep(150);
    }
    return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
  }

  // --- scenarios ----------------------------------------------------------

  private static Result benchOffer(String dataDir, int size, int ops, int warmup, boolean checksum)
      throws Exception {
    byte[] data = payload(size);

    // warmup on a throwaway file
    String wf = dataDir + "/warmup.queue";
    deleteFile(wf);
    try (MessageQueue q = open(wf, checksum)) {
      for (int i = 0; i < warmup; i++) {
        q.offer(new Message(data, i));
      }
    }
    deleteFile(wf);

    String f = dataDir + "/offer.queue";
    deleteFile(f);
    MessageQueue q = open(f, checksum);

    long[] gc0 = gcSnapshot();
    long alloc0 = threadAllocatedBytes();
    long t0 = System.nanoTime();
    for (int i = 0; i < ops; i++) {
      if (!q.offer(new Message(data, i))) {
        throw new IllegalStateException("offer rejected at op " + i);
      }
    }
    q.close(); // flush buffered writes; part of the timed region
    long t1 = System.nanoTime();
    long alloc1 = threadAllocatedBytes();
    long[] gc1 = gcSnapshot();
    deleteFile(f);

    long elapsed = t1 - t0;
    return new Result()
        .put("elapsed_ns", elapsed)
        .put("msgs_per_sec", ops * 1e9 / elapsed)
        .put("mib_per_sec", (double) ops * size * 1e9 / elapsed / (1024 * 1024))
        .put("alloc_bytes_per_op", (alloc1 - alloc0) / (double) ops)
        .put("gc_count", gc1[0] - gc0[0])
        .put("gc_time_ms", gc1[1] - gc0[1]);
  }

  private static Result benchPoll(String dataDir, int size, int ops, int warmup, boolean checksum)
      throws Exception {
    byte[] data = payload(size);

    // warmup: full produce/consume cycle on a throwaway file
    String wf = dataDir + "/warmup.queue";
    deleteFile(wf);
    try (MessageQueue q = open(wf, checksum)) {
      for (int i = 0; i < warmup; i++) {
        q.offer(new Message(data, i));
      }
    }
    try (MessageQueue q = open(wf, checksum)) {
      for (int i = 0; i < warmup; i++) {
        sink += q.poll().getData()[0];
      }
    }
    deleteFile(wf);

    // populate (not timed)
    String f = dataDir + "/poll.queue";
    deleteFile(f);
    try (MessageQueue q = open(f, checksum)) {
      for (int i = 0; i < ops; i++) {
        q.offer(new Message(data, i));
      }
    }

    MessageQueue q = open(f, checksum);
    long[] gc0 = gcSnapshot();
    long alloc0 = threadAllocatedBytes();
    long t0 = System.nanoTime();
    for (int i = 0; i < ops; i++) {
      Message m = q.poll();
      if (m == null) {
        throw new IllegalStateException("queue empty at op " + i);
      }
      sink += m.getData()[0];
    }
    long t1 = System.nanoTime();
    long alloc1 = threadAllocatedBytes();
    long[] gc1 = gcSnapshot();

    // steady-state heap after a full produce/consume cycle, queue still open
    long heapUsed = settledHeapUsed();
    q.close();
    deleteFile(f);

    long elapsed = t1 - t0;
    return new Result()
        .put("elapsed_ns", elapsed)
        .put("msgs_per_sec", ops * 1e9 / elapsed)
        .put("mib_per_sec", (double) ops * size * 1e9 / elapsed / (1024 * 1024))
        .put("alloc_bytes_per_op", (alloc1 - alloc0) / (double) ops)
        .put("gc_count", gc1[0] - gc0[0])
        .put("gc_time_ms", gc1[1] - gc0[1])
        .put("heap_after_cycle_bytes", heapUsed);
  }

  private static Result benchLatency(String dataDir, int size, int ops, int warmup, boolean checksum)
      throws Exception {
    byte[] data = payload(size);
    String f = dataDir + "/latency.queue";
    deleteFile(f);
    MessageQueue q = open(f, checksum);

    for (int i = 0; i < warmup; i++) {
      q.offer(new Message(data, i));
      sink += q.poll().getData()[0];
    }

    long[] samples = new long[ops];
    long[] gc0 = gcSnapshot();
    long alloc0 = threadAllocatedBytes();
    for (int i = 0; i < ops; i++) {
      long t0 = System.nanoTime();
      q.offer(new Message(data, i));
      Message m = q.poll();
      long t1 = System.nanoTime();
      sink += m.getData()[0];
      samples[i] = t1 - t0;
    }
    long alloc1 = threadAllocatedBytes();
    long[] gc1 = gcSnapshot();
    q.close();
    deleteFile(f);

    Arrays.sort(samples);
    return new Result()
        .put("p50_us", samples[(int) (ops * 0.50)] / 1e3)
        .put("p95_us", samples[(int) (ops * 0.95)] / 1e3)
        .put("p99_us", samples[(int) (ops * 0.99)] / 1e3)
        .put("max_us", samples[ops - 1] / 1e3)
        .put("mean_us", Arrays.stream(samples).average().orElse(0) / 1e3)
        .put("alloc_bytes_per_op", (alloc1 - alloc0) / (double) ops)
        .put("gc_count", gc1[0] - gc0[0])
        .put("gc_time_ms", gc1[1] - gc0[1]);
  }

  private static Result benchOpenClose(String dataDir, int size, int cycles, int warmup, boolean checksum)
      throws Exception {
    byte[] data = payload(size);
    int populate = 5000;

    String f = dataDir + "/openclose.queue";
    deleteFile(f);
    try (MessageQueue q = open(f, checksum)) {
      for (int i = 0; i < populate; i++) {
        q.offer(new Message(data, i));
      }
    }

    for (int i = 0; i < warmup; i++) {
      open(f, checksum).close();
    }

    long[] samples = new long[cycles];
    for (int i = 0; i < cycles; i++) {
      long t0 = System.nanoTime();
      MessageQueue q = open(f, checksum);
      q.close();
      long t1 = System.nanoTime();
      samples[i] = t1 - t0;
    }
    deleteFile(f);

    Arrays.sort(samples);
    return new Result()
        .put("p50_us", samples[(int) (cycles * 0.50)] / 1e3)
        .put("p95_us", samples[(int) (cycles * 0.95)] / 1e3)
        .put("p99_us", samples[(int) (cycles * 0.99)] / 1e3)
        .put("mean_us", Arrays.stream(samples).average().orElse(0) / 1e3)
        .put("populated_msgs", populate);
  }
}
