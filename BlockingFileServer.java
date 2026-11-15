package filetransfer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * Traditional blocking server, written only so we can COMPARE it with the NIO
 * server. Uses java.io streams and starts a NEW THREAD for EVERY client.
 * Same protocol, same upload folder logic.
 */
public class BlockingFileServer extends FileServer {

    private ServerSocket serverSocket;
    private int activeThreads;
    private int peakThreads;

    public BlockingFileServer(int port, Path uploadDir) {
        super(port, uploadDir);
    }

    @Override
    public String getName() {
        return "BLOCKING";
    }

    @Override
    protected void bind() throws IOException {
        serverSocket = new ServerSocket(port, 200);
    }

    @Override
    protected void serve() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();          // blocks until a client connects
                int id = nextClientId();
                Thread t = new Thread(() -> handle(socket, id)); // lambda: one thread per client
                t.start();
            } catch (IOException e) {
                if (running) {
                    log("accept failed: " + e.getMessage());
                }
            }
        }
    }

    @Override
    protected void unblock() {
        try {
            serverSocket.close();                               // makes accept() throw
        } catch (IOException ignored) {
            // ignore
        }
    }

    private void handle(Socket socket, int id) {
        threadStarted();
        try (Socket s = socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
             OutputStream out = s.getOutputStream()) {

            byte[] raw = new byte[Protocol.HEADER_SIZE];
            in.readFully(raw);                                  // blocks until header is complete
            ByteBuffer header = ByteBuffer.wrap(raw);
            String name = Protocol.decodeName(header);
            long size = Protocol.decodeSize(header);
            if (size < 0) {
                throw new TransferException("Negative file size");
            }
            Path target = resolveTarget(id, name);

            try (OutputStream file = new BufferedOutputStream(new FileOutputStream(target.toFile()))) {
                byte[] buf = new byte[8192];
                long left = size;
                while (left > 0) {
                    int n = in.read(buf, 0, (int) Math.min(buf.length, left));  // blocks
                    if (n == -1) {
                        throw new EOFException("Client #" + id + " disconnected mid-transfer");
                    }
                    file.write(buf, 0, n);
                    left -= n;
                }
            }
            recordFile(size);
            out.write(Protocol.ACK.getBytes(StandardCharsets.UTF_8));
            out.flush();
        } catch (IOException e) {
            log("client #" + id + " error: " + e.getMessage());
        } finally {
            threadFinished();
        }
    }

    // synchronized: many handler threads update these counters at once
    private synchronized void threadStarted() {
        activeThreads++;
        peakThreads = Math.max(peakThreads, activeThreads);
    }

    private synchronized void threadFinished() {
        activeThreads--;
    }

    @Override
    public synchronized int getPeakServerThreads() {
        return peakThreads + 1;                                 // handlers + the accepting thread
    }

    @Override
    public synchronized int getPeakClients() {
        return peakThreads;
    }
}
