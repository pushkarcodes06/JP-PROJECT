package filetransfer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract base class. It holds what both servers have in common and
 * leaves the "how do we handle many clients" part to the subclasses:
 *
 *   NioFileServer      - one thread + Selector (non-blocking)
 *   BlockingFileServer - one thread per client (classic blocking I/O)
 */
public abstract class FileServer implements Runnable, Monitorable {

    protected final int port;
    protected final Path uploadDir;
    protected volatile boolean running;

    private final AtomicInteger filesReceived = new AtomicInteger();
    private final AtomicLong bytesReceived = new AtomicLong();
    private final AtomicInteger clientCounter = new AtomicInteger();
    private Thread worker;

    public FileServer(int port, Path uploadDir) {
        this.port = port;
        this.uploadDir = uploadDir;
    }

    // Constructor overloading
    public FileServer(int port) {
        this(port, Paths.get("uploads"));
    }

    // ---- parts that each subclass must supply (abstract methods) ----
    protected abstract void bind() throws IOException;   // open the listening socket
    protected abstract void serve();                      // main loop
    protected abstract void unblock();                    // wake the main loop up so it can end
    public abstract String getName();

    /** Default: no per-client list. NIO server overrides this. */
    public List<String> describeClients() {
        return new ArrayList<>();
    }

    // ---- common behaviour ----
    public void start() throws IOException {
        Files.createDirectories(uploadDir);
        bind();
        running = true;
        worker = new Thread(this, getName() + "-main");
        worker.start();
        log("listening on port " + port + ", saving to " + uploadDir.toAbsolutePath());
    }

    @Override
    public void run() {
        serve();
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        unblock();
        try {
            worker.join(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log("stopped. " + stats());
    }

    protected int nextClientId() {
        return clientCounter.incrementAndGet();
    }

    protected void recordFile(long bytes) {
        filesReceived.incrementAndGet();
        bytesReceived.addAndGet(bytes);
    }

    // Method overloading: two versions of resolveTarget
    protected Path resolveTarget(String name) throws TransferException {
        try {
            Path leaf = Paths.get(name).getFileName();   // strips any folders: no "../" tricks
            if (leaf == null || leaf.toString().isEmpty()) {
                throw new TransferException("Invalid file name: '" + name + "'");
            }
            return uploadDir.resolve(leaf);
        } catch (InvalidPathException e) {
            throw new TransferException("Invalid file name: '" + name + "'");
        }
    }

    /** Prefixes the client id so two clients sending the same name don't overwrite each other. */
    protected Path resolveTarget(int clientId, String name) throws TransferException {
        String leaf = resolveTarget(name).getFileName().toString();
        return uploadDir.resolve("client" + clientId + "_" + leaf);
    }

    protected void log(String message) {
        System.out.println("[" + getName() + "] " + message);
    }

    @Override
    public String stats() {
        return getName() + ": files=" + filesReceived.get()
                + ", bytes=" + bytesReceived.get()
                + ", peakThreads=" + getPeakServerThreads()
                + ", peakClients=" + getPeakClients();
    }

    public int getFilesReceived() {
        return filesReceived.get();
    }

    public long getBytesReceived() {
        return bytesReceived.get();
    }

    public Path getUploadDir() {
        return uploadDir;
    }
}
