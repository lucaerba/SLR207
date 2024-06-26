package rs;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Node {
    private final static boolean DEBUG = true;
    private final static String MAP_MSG = "MAP";
    private final static String REDUCE_MSG = "REDUCE";
    private final static String REDUCE2_MSG = "REDUCE2s";
    private final static String IP_MSG_N = "IPSN";
    private final static String IP_MSG_IP = "IPSI";
    private final static String GROUP_MSG = "GROUP";
    protected static String username = "toto";
    protected static String homeDirectory = System.getProperty("java.io.tmpdir") + "/lucaerbi/" + username;
    
    public static void main(String[] args) {
        ServerSocket listener = null;
        String line;
        BufferedReader is;
        BufferedWriter os;
        Socket socketOfServer = null;
        MySocketClient socketClient = null;
        MyFTPServer ftpServer = null;
        MyFTPClient ftpClient = null;
        String[] ips = null;
        String clientip = null;
        int clientport = 1234;
        Map<String, Integer> result = null;
        int low_range, high_range; //[low_range, high_range)
        int server_index = -1;
        // Try to open a server socket on port 9999
        // Note that we can't choose a port less than 1023 if we are not
        // privileged users (root)

        //take port from command line
        int port = 4567;
        
        try {
            listener = new ServerSocket(port);
            ftpServer = new MyFTPServer(port+100);
            socketClient = new MySocketClient();
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

                    result = my_map(filename);
                    
                    System.out.println("mapping " + filename + " result: " + result.size() + " words");
                    //write the results to the others servers using the ftp client
                    StringBuilder content = new StringBuilder();
                    for (Map.Entry<String, Integer> stringIntegerEntry : result.entrySet()) {
                        content.append(stringIntegerEntry.getKey()).append(" ").append(stringIntegerEntry.getValue()).append("\n");
                    }
                    //just keep
                    String name_file = filename.split("/")[filename.split("/").length-1];
                    name_file = name_file.split("_")[0];
                    name_file += ("_shuffle_") ;
                    ftpClient.saveResultsOnServers(name_file+server_index, content.toString(), server_index);
                    //filter result and keep only the words that hash % n_server == i

                    final int nServer = n_server;
                    final int iServer = server_index;
                    if(DEBUG){
                        System.out.println("Result: ");
                        //print the map result
                        for (Map.Entry<String, Integer> stringIntegerEntry : result.entrySet()) {
                            System.out.println(stringIntegerEntry.getKey() + " " + stringIntegerEntry.getValue());
                        }
                    }

                    result = result.entrySet().stream()
                            .filter(entry -> entry.getKey().hashCode() % nServer == iServer)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    if(DEBUG){
                        System.out.println("Result: ");
                        //print the map result
                        for (Map.Entry<String, Integer> stringIntegerEntry : result.entrySet()) {
                            System.out.println(stringIntegerEntry.getKey() + " " + stringIntegerEntry.getValue());
                        }
                    }
                    //get the others string from the others server reading filne name_file
                    for(int i=0; i<n_server; i++) {
                        if(i == server_index) continue;
                        String wait_file_name = name_file + i;
                        Path filePath = Paths.get(homeDirectory + "/" + wait_file_name);
                        System.out.println("Waiting for file " + filePath.toString());
                        while(!Files.exists(filePath));
                        //append to results
                        Map<String, Integer> newMap = my_map(filePath.toString());
                        for (String key : newMap.keySet()) {
                            if (result.containsKey(key)) {
                                // If the key exists, add the new value to the existing value
                                result.put(key, result.get(key) + newMap.get(key));
                            } else {
                                // If the key doesn't exist, just put the new value
                                result.put(key, newMap.get(key));
                            }
                        }
                    }
                    if(DEBUG){
                        System.out.println("Result: ");
                        for (Map.Entry<String, Integer> stringIntegerEntry : result.entrySet()) {
                            System.out.println(stringIntegerEntry.getKey() + " " + stringIntegerEntry.getValue());
                        }
                    }
                    socketClient.sendMsgToServer(clientip, clientport, "FINISHED");
                    System.out.println("MAP finished");
                    
                } else if(line.equals(REDUCE_MSG)) {
                    System.out.println("REDUCE message received");
                    //calculate fmin and fmax from the frequency list [key:occurrences]
                    int fmin = Integer.MAX_VALUE;
                    int fmax = Integer.MIN_VALUE;
                    for (Map.Entry<String, Integer> stringIntegerEntry : result.entrySet()) {
                        int value = stringIntegerEntry.getValue();
                        if (value < fmin) {
                            fmin = value;
                        }
                        if (value > fmax) {
                            fmax = value;
                        }
                    }
                    System.out.println("fmin: " + fmin + " fmax: " + fmax);
                    //take from all the files (local file, filename up to .txt_i_n_server) the words with hash % n_server == i and keep them as result

                    //send the result to the client
                    socketClient.sendMsgToServer(clientip, clientport, "" + fmin + " " + fmax);

                } else if(line.contains(IP_MSG_N)) {
                    System.out.println("IPS message received n");
                    //get the ips of the other servers
                    String serverCountLine = line.split(" ")[1];
                    String indexLine = line.split(" ")[2];
                    //clientip = line.split(" ")[2];
                    n_server = Integer.parseInt(serverCountLine);
                    server_index = Integer.parseInt(indexLine);
                    System.out.println("Number of servers: " + n_server + " my index: " + server_index);
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
                        clientip = socketOfServer.getInetAddress().getHostAddress();
                    }
                }else if(line.contains(GROUP_MSG)) {
                    System.out.println("GROUP message received");
                    //get the ranges GRPOUP fmin fmax
                    String[] ranges = line.split(" ");
                    low_range = Integer.parseInt(ranges[1]);
                    high_range = Integer.parseInt(ranges[2]);

                    System.out.println("Mine frequencies between " + low_range + " and " + high_range);

                    //do the shuffle, keep words with frequency between low_range and high_range and send the others to the other servers



                    //send shuffle2 finished to the client
                    socketClient.sendMsgToServer(clientip, clientport, "SHUFFLE2 FINISHED");
                }else if(line.equals(REDUCE2_MSG)) {
                    System.out.println("REDUCE2 message received");

                    
                }else {
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
    private static Map<String, Integer> my_map(String filename) {
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
            //remove "" from the list
            wordCount.remove("");
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return wordCount;
    }
}
