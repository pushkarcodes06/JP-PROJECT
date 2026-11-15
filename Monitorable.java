package filetransfer;

/** Simple interface: anything that can report statistics about itself. */
public interface Monitorable {

    /** Highest number of threads the server needed to talk to its clients. */
    int getPeakServerThreads();

    /** Highest number of clients connected at the same time. */
    int getPeakClients();

    /** One-line summary. */
    String stats();
}
