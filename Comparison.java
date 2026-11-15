package filetransfer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Evaluation: runs the SAME workload against the NIO server and the blocking
 * server and prints a comparison table.
 *
 * Usage: java -cp out filetransfer.Comparison [clients=50] [fileKB=512] [delayMs=5]
 *
 * Note: the client threads live in this same JVM, so we do not compare total JVM
 * threads. We compare the threads the SERVER needed (reported by the server).
 */
public class Comparison {

    /** Static nested class: just a bag of results for one server. */
    static class Result {
        final String mode;
        final int clients;
        final int filesOk;
        final long millis;
        final int serverThreads;
        final int peakClients;
        final boolean identical;

        Result(String mode, int clients, int filesOk, long millis,
               int serverThreads, int peakClients, boolean identical) {
            this.mode = mode;
            this.clients = clients;
            this.filesOk = filesOk;
            this.millis = millis;
            this.serverThreads = serverThreads;
            this.peakClients = peakClients;
            this.identical = identical;
        }
    }

    public static void main(String[] args) throws Exception {
        int clientCount = args.length > 0 ? Integer.parseInt(args[0]) : 50;
        int fileKb = args.length > 1 ? Integer.parseInt(args[1]) : 512;
        int delayMs = args.length > 2 ? Integer.parseInt(args[2]) : 5;

        Path work = Files.createTempDirectory("filetransfer-demo");
        Path sourceDir = Files.createDirectory(work.resolve("source"));
        List<Path> sources = createTestFiles(sourceDir, clientCount, fileKb);

        System.out.println("Workload: " + clientCount + " clients, each uploading one "
                + fileKb + " KB file, " + delayMs + " ms pause per 8 KB chunk (slow network)\n");

        // Warm-up round (result ignored) so the JVM has compiled the hot code
        // before we measure - otherwise whoever runs first looks slower.
        System.out.println("Warm-up round...");
        runTest(new NioFileServer(5003, work.resolve("warm-nio")), sources, delayMs);
        runTest(new BlockingFileServer(5004, work.resolve("warm-blocking")), sources, delayMs);
        System.out.println("\nMeasured round...");

        List<FileServer> servers = new ArrayList<>();
        servers.add(new NioFileServer(5001, work.resolve("nio-out")));
        servers.add(new BlockingFileServer(5002, work.resolve("blocking-out")));

        List<Result> results = new ArrayList<>();
        for (FileServer server : servers) {
            results.add(runTest(server, sources, delayMs));
        }

        System.out.println();
        printTable(results);
        System.out.println();
        printConclusion(results.get(0), results.get(1));
    }

    private static Result runTest(FileServer server, List<Path> sources, int delayMs) throws Exception {
        server.start();

        List<FileClient> clients = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        long begin = System.nanoTime();
        for (Path source : sources) {
            FileClient client = new FileClient("localhost", server.port, source, delayMs);
            Thread thread = new Thread(client);
            clients.add(client);
            threads.add(thread);
            thread.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        long millis = (System.nanoTime() - begin) / 1_000_000;
        server.stop();

        int ok = (int) clients.stream().filter(FileClient::isSuccess).count();   // lambda-style stream
        boolean identical = verify(sources, server.getUploadDir());
        return new Result(server.getName(), sources.size(), ok, millis,
                server.getPeakServerThreads(), server.getPeakClients(), identical);
    }

    /** Every source file must exist on the server with exactly the same bytes. */
    private static boolean verify(List<Path> sources, Path outDir) throws IOException {
        for (Path source : sources) {
            String suffix = "_" + source.getFileName();
            List<Path> match;
            try (Stream<Path> files = Files.list(outDir)) {
                match = files.filter(p -> p.getFileName().toString().endsWith(suffix))
                             .collect(Collectors.toList());
            }
            if (match.size() != 1 || Files.mismatch(source, match.get(0)) != -1L) {
                return false;
            }
        }
        return true;
    }

    private static List<Path> createTestFiles(Path dir, int count, int kb) throws IOException {
        List<Path> files = new ArrayList<>();
        Random random = new Random(42);
        for (int i = 1; i <= count; i++) {
            byte[] data = new byte[kb * 1024];
            random.nextBytes(data);
            files.add(Files.write(dir.resolve("file_" + i + ".bin"), data));
        }
        return files;
    }

    private static void printTable(Collection<Result> results) {
        System.out.printf("%-10s %8s %10s %14s %14s %10s%n",
                "Server", "Uploads", "Time (ms)", "Server threads", "Peak clients", "Files OK");
        System.out.println("-".repeat(72));
        for (Result r : results) {
            System.out.printf("%-10s %5d/%-3d %10d %14d %14d %10s%n",
                    r.mode, r.filesOk, r.clients, r.millis, r.serverThreads, r.peakClients,
                    r.identical ? "yes" : "NO");
        }
    }

    private static void printConclusion(Result nio, Result blocking) {
        System.out.println("Observations");
        System.out.println(" * NIO handled up to " + nio.peakClients + " simultaneous clients with "
                + nio.serverThreads + " thread; blocking I/O needed " + blocking.serverThreads + ".");
        System.out.println(" * Time: NIO " + nio.millis + " ms, blocking " + blocking.millis + " ms.");
        System.out.println(" * Both servers received every file byte-for-byte: "
                + (nio.identical && blocking.identical));
    }
}
