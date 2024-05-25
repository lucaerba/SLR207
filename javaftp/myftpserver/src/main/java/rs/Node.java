package rs;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Node {
    private final static String MAP_MSG = "MAP";
    private final static String REDUCE_MSG = "REDUCE";

    public static void main(String[] args) {
        ServerSocket listener = null;
        String line;
        BufferedReader is;
        BufferedWriter os;
        Socket socketOfServer = null;
        MyFTPServer ftpServer = null;
        MyFTPClient ftpClient = null;
        // Try to open a server socket on port 9999
        // Note that we can't choose a port less than 1023 if we are not
        // privileged users (root)

        //take port from command line
        int port = Integer.parseInt(args[0]);
        
        try {
            listener = new ServerSocket(port+100);
            ftpServer = new MyFTPServer(port+101);
        } catch (IOException e) {
            System.out.println(e);
            System.exit(1);
        }

        try {
            System.out.println("Server is waiting to accept user...");

            // Accept client connection request
            // Get new Socket at Server.
            socketOfServer = listener.accept();
            System.out.println("Accept a client!");

            // Open input and output streams
            is = new BufferedReader(new InputStreamReader(socketOfServer.getInputStream()));
            os = new BufferedWriter(new OutputStreamWriter(socketOfServer.getOutputStream()));
            

            while (true) {
                // Read data to the server (sent from client).
                line = is.readLine();
                if(line.contains(MAP_MSG)) {
                    System.out.println("MAP message received");

                    String filename = line.split(" ")[1];
                    filename = homeDirectory + "/" + filename;
                    List<Map.Entry<String, Integer>> result = my_map(filename);

                    //write all the results to the client
                    for (Map.Entry<String, Integer> entry : result) {
                        System.out.println("map result: " + entry.getKey() + " " + entry.getValue());
                        os.write(entry.getKey() + " " + entry.getValue());
                        os.newLine();
                        os.flush();
                    }
                    os.write("END");
                    os.newLine();
                    os.flush();
                } else if(line.contains(REDUCE_MSG)) {
                    System.out.println("REDUCE message received");
                } else {
                    System.out.println("Message received: " + line);
                }

            }

        } catch (IOException e) {
            System.out.println(e);
            e.printStackTrace();
        }
        System.out.println("Sever stopped!");
    }


    // return a list of string occurrencies
    private static List<Map.Entry<String, Integer>> my_map(String filename) {
        HashMap<String, Integer> wordCount = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] words = line.split(" ");
                for (String word : words) {
                    wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>(wordCount.entrySet());
    }
}
