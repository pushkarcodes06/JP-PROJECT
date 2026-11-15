package filetransfer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.StringTokenizer;

/**
 * Starts a server and gives a tiny console.
 *
 * Usage: java -cp out filetransfer.ServerMain [nio|blocking] [port] [folder]
 * Type:  stats | clients | quit
 */
public class ServerMain {

    public static void main(String[] args) {
        String mode = args.length > 0 ? args[0].toLowerCase() : "nio";
        int port;
        try {
            port = args.length > 1 ? Integer.parseInt(args[1]) : 5000;
        } catch (NumberFormatException e) {
            System.out.println("Port must be a number");
            return;
        }
        Path dir = Paths.get(args.length > 2 ? args[2] : "uploads");

        final FileServer server = mode.equals("blocking")
                ? new BlockingFileServer(port, dir)
                : new NioFileServer(port, dir);

        try {
            server.start();
        } catch (IOException e) {
            System.out.println("Could not start server: " + e.getMessage());
            return;
        }

        // Anonymous inner class: stop the server cleanly on Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.stop();
            }
        });

        System.out.println("Commands: stats | clients | quit");
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            StringTokenizer tokens = new StringTokenizer(scanner.nextLine());
            if (!tokens.hasMoreTokens()) {
                continue;
            }
            String command = tokens.nextToken().toLowerCase();
            if (command.equals("quit")) {
                break;
            } else if (command.equals("stats")) {
                System.out.println(server.stats());
            } else if (command.equals("clients")) {
                server.describeClients().forEach(System.out::println);   // method reference
            } else {
                System.out.println("Unknown command: " + command);
            }
        }
        server.stop();
    }
}
