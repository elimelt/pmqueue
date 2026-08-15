# pmqueue - Persistent Message Queue

`pmqueue` is a simple persistent message queue written in Java (no dependencies).

## Quickstart

```java
import io.github.elimelt.pmqueue.MessageQueue;
import io.github.elimelt.pmqueue.QueueFactory;
import io.github.elimelt.pmqueue.message.Message;

try (MessageQueue queue = QueueFactory.createQueue("path/to/queue.dat")) {
    queue.offer(new Message("Hello, World!".getBytes(), 1));
    Message message = queue.poll();
    System.out.println(new String(message.getData()));
}
```

`offer`, `poll`, and `close` throw `IOException`. `QueueFactory` also provides
`createHighThroughputQueue`, `createDurableQueue`, `createLargeMessageQueue`,
`createLowMemoryQueue`, and `createDebugQueue`, each taking a file path.

## Build and test

Requires JDK 21+.

```
./run_tests.sh
```

This downloads a JDK and test dependencies into `target/` on first run, then
compiles and runs the test suite. No Maven install needed.

Alternatively, with Maven installed:

```
mvn test
```

## Docs

Full docs: https://elimelt.com/pmqueue/
