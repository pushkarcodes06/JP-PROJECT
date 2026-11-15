package filetransfer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * One client = one file upload. Reads the file with a FileChannel into a
 * ByteBuffer and writes the buffer to a SocketChannel.
 *
 * Implements Runnable so many clients can run at the same time, each in a Thread.
 *
 * Usage:  java -cp out filetransfer.FileClient <host> <port> <file1> [file2 ...]
 *         (every file is uploaded by its own client thread)
 */
public class FileClient implements Runnable {

    private final String host;
    private final int port;
    private final Path file;
    private final int delayMs;            // pause after each chunk (simulates a slow network)

    private volatile boolean success;
    private volatile String error = "";

    public FileClient(String host, int port, Path file, int delayMs) {
        this.host = host;
        this.port = port;
        this.file = file;
        this.delayMs = delayMs;
    }

    // Constructor overloading: no delay by default
    public FileClient(String host, int port, Path file) {
        this(host, port, file, 0);
    }

    @Override
    public void run() {
        try {
            upload();
            success = true;
        } catch (IOException | InterruptedException e) {
            error = e.getMessage();
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void upload() throws IOException, InterruptedException {
        try (FileChannel source = FileChannel.open(file, StandardOpenOption.READ);
             SocketChannel server = SocketChannel.open(new InetSocketAddress(host, port))) {

            // 1. header (name + size)
            ByteBuffer header = Protocol.encodeHeader(file.getFileName().toString(), source.size());
            while (header.hasRemaining()) {
                server.write(header);
            }

            // 2. file content, chunk by chunk
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            while (source.read(buffer) != -1) {          // file -> buffer
                buffer.flip();
                while (buffer.hasRemaining()) {
                    server.write(buffer);                // buffer -> socket
                }
                buffer.clear();
                if (delayMs > 0) {
                    Thread.sleep(delayMs);
                }
            }

            // 3. wait for the server's "OK"
            ByteBuffer ack = ByteBuffer.allocate(Protocol.ACK.length());
            while (ack.hasRemaining()) {
                if (server.read(ack) == -1) {
                    throw new TransferException("Server closed without acknowledging");
                }
            }
            String reply = new String(ack.array(), StandardCharsets.UTF_8);
            if (!Protocol.ACK.equals(reply)) {
                throw new TransferException("Unexpected reply: " + reply);
            }
        }
    }

    public boolean isSuccess() { return success; }
    public String getError() { return error; }
    public Path getFile() { return file; }

    // ------------------------------------------------------------------

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 3) {
            System.out.println("Usage: FileClient <host> <port> <file1> [file2 ...]");
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);

        List<FileClient> clients = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            Path path = Paths.get(args[i]);
            if (!Files.isRegularFile(path)) {
                System.out.println("Skipping (not a file): " + path);
                continue;
            }
            FileClient client = new FileClient(host, port, path);
            Thread thread = new Thread(client);
            clients.add(client);
            threads.add(thread);
            thread.start();                              // all uploads run at the same time
        }
        for (Thread t : threads) {
            t.join();
        }
        for (FileClient c : clients) {
            System.out.println(c.getFile().getFileName() + " : "
                    + (c.isSuccess() ? "uploaded" : "FAILED (" + c.getError() + ")"));
        }
    }
}
