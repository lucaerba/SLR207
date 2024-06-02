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
    private final static String IP_MSG_N = "IPSN";
    private final static String IP_MSG_IP = "IPSI";
    protected static String username = "toto";
    protected static String homeDirectory = System.getProperty("java.io.tmpdir") + "/lucaerbi/" + username;
    public static void main(String[] args) {
        ServerSocket listener = null;
        String line;
        BufferedReader is;
        BufferedWriter os;
        Socket socketOfServer = null;
        MyFTPServer ftpServer = null;
        MyFTPClient ftpClient = null;
        String[] ips = null;
        // Try to open a server socket on port 9999
        // Note that we can't choose a port less than 1023 if we are not
        // privileged users (root)

        //take port from command line
        int port = 4569;
        
        try {
            listener = new ServerSocket(port);
            ftpServer = new MyFTPServer(port+100);
        } catch (IOException e) {
            System.out.println(e);
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            System.out.println("Server is waiting to accept user...");

            
            int n_server = 0;
            while (true) {
                // Accept client connection request
                // Get new Socket at Server.
                socketOfServer = listener.accept();
                System.out.println("Accept a client!");

                // Open input and output streams
                is = new BufferedReader(new InputStreamReader(socketOfServer.getInputStream()));
                os = new BufferedWriter(new OutputStreamWriter(socketOfServer.getOutputStream()));
                
                // Read data to the server (sent from client).
                line = is.readLine();
                if(line.contains(MAP_MSG)) {
                    System.out.println("MAP message received");

                    String filename = line.split(" ")[1];
                    filename = homeDirectory + "/" + filename;
                    List<Map.Entry<String, Integer>> result = my_map(filename);
                    System.out.println("mapping " + filename + " result: " + result.size() + " words");
                    //write all the results to the others servers using the ftp client
                    for(int i=0; i<n_server; i++) {
                        String content = "";
                        for(int j=0; j<result.size(); j++) {
                            content += result.get(j).getKey() + " " + result.get(j).getValue() + "\n";
                        }
                        ftpClient.saveResultsOnServers(filename, content);
                    }
                    os.write("MAP"+port+" SHUFFLE"+port+" FINISHED");
                    os.newLine();
                    os.flush();
                } else if(line.contains(REDUCE_MSG)) {
                    System.out.println("REDUCE message received");
                } else if(line.contains(IP_MSG_N)) {
                    System.out.println("IPS message received n");
                    //get the ips of the other servers
                    String serverCountLine = line.split(" ")[1];
                    n_server = Integer.parseInt(serverCountLine);
                    System.out.println("Number of servers: " + n_server);
                    ips = new String[n_server-1];
                    n_server = 0;
                }else if(line.contains(IP_MSG_IP)) {
                    System.out.println("IPS message received ip");
                    String ipLine = line.split(" ")[1];
                    System.out.println("Server " + n_server + ": " + ipLine);
                    ips[n_server] = ipLine;
                    n_server++;
                    if (n_server == ips.length) {
                        ftpClient = new MyFTPClient(ips);
                    }
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
