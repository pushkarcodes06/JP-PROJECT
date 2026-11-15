package filetransfer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Non-blocking server: ONE thread watches ALL clients with a Selector.
 *
 *   Selector --- tells us which channels are ready (accept / read)
 *   Channel  --- ServerSocketChannel, SocketChannel, FileChannel
 *   Buffer   --- ByteBuffer holds the bytes moving between channels
 *   HashMap  --- SocketChannel -> ClientInfo (progress of every client)
 */
public class NioFileServer extends FileServer {

    private Selector selector;
    private ServerSocketChannel serverChannel;

    // Connection details of every connected client
    private final Map<SocketChannel, ClientInfo> clients = new HashMap<>();
    private final ByteBuffer buffer = ByteBuffer.allocate(8192);
    private int peakClients;

    public NioFileServer(int port, Path uploadDir) {
        super(port, uploadDir);
    }

    @Override
    public String getName() {
        return "NIO";
    }

    @Override
    protected void bind() throws IOException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(port), 200);   // 200 = connection backlog
        serverChannel.configureBlocking(false);                 // IMPORTANT: non-blocking
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    protected void serve() {
        try {
            while (running) {
                selector.select();                              // sleeps until something is ready
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (!key.isValid()) {
                        continue;
                    }
                    try {
                        if (key.isAcceptable()) {
                            accept();
                        } else if (key.isReadable()) {
                            read(key);
                        }
                    } catch (IOException e) {                   // one bad client must not stop the others
                        log("error: " + e.getMessage());
                        closeClient(key);
                    }
                }
            }
        } catch (IOException e) {
            log("selector failed: " + e.getMessage());
        } finally {
            closeEverything();
        }
    }

    @Override
    protected void unblock() {
        selector.wakeup();                                      // makes select() return
    }

    // ------------------------------------------------------------------

    private void accept() throws IOException {
        SocketChannel channel;
        while ((channel = serverChannel.accept()) != null) {    // drain every waiting connection
            channel.configureBlocking(false);
            ClientInfo info = new ClientInfo(nextClientId(), channel);
            synchronized (clients) {
                clients.put(channel, info);
                peakClients = Math.max(peakClients, clients.size());
            }
            channel.register(selector, SelectionKey.OP_READ);
        }
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientInfo info;
        synchronized (clients) {
            info = clients.get(channel);
        }
        if (info == null) {
            closeClient(key);
            return;
        }

        if (!info.isHeaderDone()) {
            readHeader(channel, info);
            if (!info.isHeaderDone()) {
                return;                                         // header not complete yet
            }
        } else {
            readBody(channel, info);
        }

        if (info.isComplete()) {
            finish(key, channel, info);
        }
    }

    private void readHeader(SocketChannel channel, ClientInfo info) throws IOException {
        ByteBuffer header = info.getHeader();
        if (channel.read(header) == -1) {
            throw new TransferException("Client #" + info.getId() + " left before sending a header");
        }
        if (header.hasRemaining()) {
            return;
        }
        String name = Protocol.decodeName(header);
        long size = Protocol.decodeSize(header);
        if (size < 0) {
            throw new TransferException("Negative file size from client #" + info.getId());
        }
        info.startFile(name, size, resolveTarget(info.getId(), name));
        log("Client #" + info.getId() + " sends '" + name + "' (" + size + " bytes)");
    }

    private void readBody(SocketChannel channel, ClientInfo info) throws IOException {
        buffer.clear();
        if (info.remaining() < buffer.capacity()) {
            buffer.limit((int) info.remaining());               // never read past this file
        }
        int n = channel.read(buffer);                           // channel -> buffer
        if (n == -1) {
            throw new TransferException("Client #" + info.getId() + " disconnected mid-transfer");
        }
        buffer.flip();
        while (buffer.hasRemaining()) {
            info.getOut().write(buffer);                        // buffer -> file channel
        }
        info.addReceived(n);
    }

    private void finish(SelectionKey key, SocketChannel channel, ClientInfo info) throws IOException {
        info.close();
        recordFile(info.getExpectedSize());
        channel.write(ByteBuffer.wrap(Protocol.ACK.getBytes(StandardCharsets.UTF_8)));
        log("Client #" + info.getId() + " done: " + info.getFileName());
        closeClient(key);
    }

    private void closeClient(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientInfo info;
        synchronized (clients) {
            info = clients.remove(channel);
        }
        if (info != null) {
            info.close();
        }
        key.cancel();
        try {
            channel.close();
        } catch (IOException ignored) {
            // already closed
        }
    }

    private void closeEverything() {
        List<SocketChannel> open;
        synchronized (clients) {
            open = new ArrayList<>(clients.keySet());
        }
        for (SocketChannel channel : open) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // ignore
            }
        }
        try {
            serverChannel.close();
            selector.close();
        } catch (IOException ignored) {
            // ignore
        }
    }

    // ------------------------------------------------------------------

    @Override
    public List<String> describeClients() {
        List<String> lines = new ArrayList<>();
        synchronized (clients) {
            for (ClientInfo info : clients.values()) {
                lines.add(info.toString());
            }
        }
        return lines;
    }

    @Override
    public int getPeakServerThreads() {
        return 1;                                               // the selector thread, always
    }

    @Override
    public int getPeakClients() {
        synchronized (clients) {
            return peakClients;
        }
    }
}
