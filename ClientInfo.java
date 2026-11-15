package filetransfer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Everything the NIO server must remember about ONE connected client.
 * Because one thread serves all clients, the progress of each client
 * (how much header / file data has arrived) is stored here, and the
 * server keeps these objects in a HashMap keyed by the client's channel.
 */
public class ClientInfo implements AutoCloseable {

    private final int id;
    private final String address;
    private final ByteBuffer header = ByteBuffer.allocate(Protocol.HEADER_SIZE);

    private boolean headerDone;
    private String fileName;
    private long expectedSize;
    private long received;
    private FileChannel out;          // where the received bytes are written

    public ClientInfo(int id, String address) {
        this.id = id;
        this.address = address;
    }

    // Constructor overloading: same class, different parameter list
    public ClientInfo(int id, SocketChannel channel) {
        this(id, remoteOf(channel));
    }

    private static String remoteOf(SocketChannel channel) {
        try {
            return String.valueOf(channel.getRemoteAddress());
        } catch (IOException e) {
            return "unknown";
        }
    }

    /** Called once the full header has arrived: opens the target file. */
    public void startFile(String name, long size, Path target) throws IOException {
        this.fileName = name;
        this.expectedSize = size;
        this.out = FileChannel.open(target,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        this.headerDone = true;
    }

    public void addReceived(int bytes) {
        received += bytes;
    }

    public long remaining() {
        return expectedSize - received;
    }

    public boolean isComplete() {
        return headerDone && received == expectedSize;
    }

    public boolean isHeaderDone() { return headerDone; }
    public ByteBuffer getHeader() { return header; }
    public FileChannel getOut() { return out; }
    public int getId() { return id; }
    public long getExpectedSize() { return expectedSize; }
    public String getFileName() { return fileName; }

    @Override
    public void close() {
        if (out != null) {
            try {
                out.close();
            } catch (IOException ignored) {
                // nothing useful to do
            }
            out = null;
        }
    }

    @Override
    public String toString() {                    // overriding Object.toString()
        String file = headerDone ? fileName + " " + received + "/" + expectedSize + " bytes"
                                 : "(waiting for header)";
        return "Client #" + id + " " + address + " -> " + file;
    }
}
