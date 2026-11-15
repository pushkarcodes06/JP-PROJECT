package filetransfer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Wire format shared by client and both servers.
 *
 *   [ 128 bytes : file name, UTF-8, padded with zeros ]
 *   [   8 bytes : file size (long)                    ]
 *   [   N bytes : file content                        ]
 *
 * After receiving all N bytes the server replies with the 2 bytes "OK".
 */
public final class Protocol {                 // 'final' -> cannot be inherited

    public static final int NAME_SIZE = 128;  // 'static final' -> constants
    public static final int HEADER_SIZE = NAME_SIZE + 8;
    public static final String ACK = "OK";

    private Protocol() { }                    // utility class, no objects

    /** Builds the header, ready to be written to a channel. */
    public static ByteBuffer encodeHeader(String fileName, long size) throws TransferException {
        byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length > NAME_SIZE) {
            throw new TransferException("File name too long: " + fileName);
        }
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.put(nameBytes);
        header.position(NAME_SIZE);
        header.putLong(size);
        header.flip();
        return header;
    }

    /** Reads the file name from a full header (absolute reads, position not needed). */
    public static String decodeName(ByteBuffer header) {
        byte[] raw = new byte[NAME_SIZE];
        for (int i = 0; i < NAME_SIZE; i++) {
            raw[i] = header.get(i);
        }
        int len = 0;
        while (len < NAME_SIZE && raw[len] != 0) {
            len++;
        }
        return new String(raw, 0, len, StandardCharsets.UTF_8);
    }

    /** Reads the file size from a full header. */
    public static long decodeSize(ByteBuffer header) {
        return header.getLong(NAME_SIZE);
    }
}
