# Multi-Client File Transfer Application (Java NIO)

Many clients upload files to one server at the same time. The main server uses
**Java NIO** (Channels, Buffers, Selector, non-blocking I/O) so a **single thread**
serves every client. A second, classic **blocking-I/O server** (one thread per
client) is included only so the two designs can be compared (BTL-5: Evaluate).

Requires Java 11 or newer (tested on Java 21). No libraries needed.

## Project structure

```
src/filetransfer/
  Protocol.java          message format shared by client and servers (final class, constants)
  TransferException.java custom exception (extends IOException)
  Monitorable.java       interface: stats of a server
  ClientInfo.java        details/progress of ONE client (stored in the HashMap)
  FileServer.java        abstract base class (common code, abstract methods)
  NioFileServer.java     NIO server: Selector + non-blocking channels + HashMap
  BlockingFileServer.java  traditional server: java.io streams + thread per client
  FileClient.java        client: FileChannel -> ByteBuffer -> SocketChannel
  ServerMain.java        starts a server, small console (stats / clients / quit)
  Comparison.java        runs the same workload on both servers, prints a table
```

## Build and run

```bash
# compile
javac -d out src/filetransfer/*.java

# 1) Start the NIO server (type  stats / clients / quit  in its console)
java -cp out filetransfer.ServerMain nio 5000 uploads

# 2) In other terminals: upload files. Every file is sent by its own client thread.
java -cp out filetransfer.FileClient localhost 5000 photo.jpg notes.pdf data.zip

# 3) Evaluation: NIO vs blocking   (clients, file size in KB, delay per chunk in ms)
java -cp out filetransfer.Comparison 50 512 5
```

`ServerMain blocking 5000 uploads` starts the blocking server instead.
Received files are saved in the upload folder as `client<id>_<name>`, so two
clients sending the same file name never overwrite each other.

## How it works

Message from client to server:

| Part | Size | Meaning |
|------|------|---------|
| file name | 128 bytes | UTF-8, padded with zeros |
| file size | 8 bytes | `long` |
| file content | size bytes | raw bytes |

The server answers `OK` (2 bytes) when the whole file is saved.

**NIO server loop (one thread):**

1. `ServerSocketChannel` is set non-blocking and registered with the `Selector` for `OP_ACCEPT`.
2. `selector.select()` sleeps until some channel is ready.
3. *Acceptable* -> accept the client, make it non-blocking, register it for `OP_READ`,
   and store a `ClientInfo` in `HashMap<SocketChannel, ClientInfo>`.
4. *Readable* -> look up the client's `ClientInfo` in the HashMap, read whatever bytes
   have arrived into a `ByteBuffer` (first the header, then the file body) and write them
   to the client's `FileChannel`. Partial data is fine: progress is kept in `ClientInfo`
   and the selector calls us again when more arrives.
5. When all bytes are received: close the file, send `OK`, remove the client from the HashMap.

An error with one client (disconnects, bad header) is caught per client and
never stops the other clients.

## Syllabus coverage

| Unit | Where it appears |
|------|------------------|
| I – classes, constructors, overloading, static/final, arrays, Scanner, StringTokenizer | `ClientInfo`, `FileServer`, `FileClient` constructors and `resolveTarget` overloads; `Protocol` (`static final`, final class); `ServerMain` (Scanner + StringTokenizer) |
| II – inheritance, abstract class, interface, overriding, inner classes, packages | `FileServer` (abstract) -> `NioFileServer` / `BlockingFileServer`; `Monitorable`; `toString()` override; `Comparison.Result` (static nested) and anonymous class in `ServerMain`; package `filetransfer` |
| III – exceptions, threads, synchronization, generics | `TransferException`, try/catch/finally; `FileClient implements Runnable`, one thread per client; `synchronized` counters; generic collections `List<FileClient>`, `Map<SocketChannel, ClientInfo>` |
| IV – collections, lambdas | `HashMap`, `ArrayList`, `Iterator` over selected keys; lambdas / method references / streams in `BlockingFileServer`, `ServerMain`, `Comparison` |
| V – I/O and NIO | `FileChannel`, `SocketChannel`, `ServerSocketChannel`, `ByteBuffer`, `Selector`, `Path`, `Files`; `java.io` streams in `BlockingFileServer` (NIO vs IO) |

## Evaluation: non-blocking NIO vs blocking I/O

Measured with `Comparison` (clients upload one file each, with a small pause between
8 KB chunks to imitate a slow network; JVM warmed up first; all files verified
byte-for-byte). Results from this project on a Linux machine, 300 clients x 128 KB:

| Server | Uploads | Time (ms) | Server threads | Peak clients |
|--------|---------|-----------|----------------|--------------|
| NIO (Selector) | 300/300 | ~180–220 | **1** | 228–260 |
| Blocking (thread per client) | 300/300 | ~240–300 | 45–64 | 44–63 |

Run the 50-client default (`Comparison` with no arguments): both servers take about the same
time (~360 ms) but NIO uses **1 thread** and blocking uses **51**.
Exact times change from run to run; the thread counts are the stable result.

### Advantages of NIO (non-blocking)

* **Few threads for many clients.** One selector thread handled up to 260 simultaneous
  connections; the blocking server needed roughly one thread per live client.
  Threads cost memory (stack, often ~1 MB each) and scheduling time, so the blocking design
  hits a limit at some thousands of clients, while NIO keeps going.
* **Slow or idle clients are cheap.** A blocking thread sits stuck in `read()` waiting
  for a slow client. With a Selector, the server only does work when data has actually arrived.
* **No locking between clients.** All client state is touched by one thread, so the
  `HashMap` needs no heavy synchronization (here only a small lock for the `clients` console command).
* **Predictable resource use** and an easy place to add limits (maximum clients, timeouts).

### Disadvantages / trade-offs of NIO

* **More complex code.** The code must be a state machine (`ClientInfo`: reading header ->
  reading body -> done) because data can arrive in any-sized pieces. Blocking code just reads in a loop.
* **One slow operation stalls everyone.** The single thread must never block (for example a very slow
  disk write); heavy work must be moved to a worker pool.
* **Little gain for few clients.** For a handful of fast connections, blocking I/O is simpler and just as quick;
  our 50-client run shows equal times.
* A single thread uses only one CPU core for the network handling; large servers run several selectors.

### Conclusion

For a file-transfer server that must accept many clients at once, the NIO design is the better
choice: it delivers the same throughput with a tiny, fixed number of threads, and it scales
with the number of connections instead of the number of threads. Blocking I/O remains
a good choice for small, simple tools with only a few clients.

## Possible extensions

* Download (server -> client) using `OP_WRITE`
* Checksum (e.g. SHA-256) sent after the file for verification
* Idle-client timeout using a time stamp in `ClientInfo`
* Asynchronous version with `AsynchronousSocketChannel` (Unit V: Asynchronous I/O)
