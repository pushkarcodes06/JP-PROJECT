package filetransfer;

import java.io.IOException;

/** Custom exception for protocol problems (bad header, early disconnect, ...). */
public class TransferException extends IOException {

    private static final long serialVersionUID = 1L;

    public TransferException(String message) {
        super(message);
    }
}
