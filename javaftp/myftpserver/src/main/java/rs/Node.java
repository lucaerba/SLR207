package rs;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.abs;

public class Node {
    private final static boolean DEBUG = false;
    private final static String MAP_MSG = "MAP";
    private final static String SHUFFLEMSG = "SHUFFLE";
    private final static String REDUCE_MSG = "REDUCE";
    private final static String REDUCE2_MSG = "REDUCE2";
    private final static String IP_MSG_N = "IPSN";
    private final static String IP_MSG_IP = "IPSI";
    private final static String GROUP_MSG = "GROUP";
    protected static String username = "toto";
    protected static String homeDirectory = System.getProperty("java.io.tmpdir") + "/lucaerbi/" + username;


    public static void deserializeMap(String serializedMap, Map<String, Integer> res) {
        String[] keyValuePairs = serializedMap.split("\n");
        for (String pair : keyValuePairs) {
            //System.out.println("Pair: " + pair);
            if (pair.contains(" ")) {
                String[] entry = pair.split(" ");
                String key = entry[0].trim();
                int value = Integer.parseInt(entry[1].trim());
                // If exists, add the new value to the existing value
                if (res.containsKey(key)) {
                    res.put(key, res.get(key) + value);
                } else {
                    // If doesn't exist, just put the new value
                    res.put(key, value);
                }
            } else {
                System.err.println("Skipping invalid pair: " + pair);
            }
        }
        return ;
    }

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
        int[] ranges = null;
        String filename = null;
        List<String> filenames = null;
        int file_index = 0;
        // Try to open a server socket on port 9999
        // Note that we can't choose a port less than 1023 if we are not
        // privileged users (root)

        //take port from command line
        int port = 4567;
        
        try {
            listener = new ServerSocket(port);
            ftpServer = new MyFTPServer(port+100);
            socketClient = new MySocketClient();
            result = new HashMap<>();
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

                    filename = line.split(" ")[1];
                    filename = homeDirectory + "/" + filename;

                    my_map(filename, result);

                    System.out.println("mapping " + filename + " result: " + result.size() + " words");


                    if (DEBUG) {
                        System.out.println("Result: ");
                        //print the map result
                        for (Map.Entry<String, Integer> stringIntegerEntry : result.entrySet()) {
                            System.out.println(stringIntegerEntry.getKey() + " " + stringIntegerEntry.getValue());
                        }
                    }

                    //just keep
                    String name_file = filename.split("/")[filename.split("/").length - 1];
                    name_file = name_file.split("_")[0];
                    name_file += ("_shuffle_");

                    filenames.add(name_file);

                    socketClient.sendMsgToServer(clientip, clientport, "FINISHED");

                }else if(line.equals(SHUFFLEMSG)) {
                    ftpClient.saveResultsOnServers_hash(filenames, result, server_index);
                    final int nServer = n_server;
                    final int iServer = server_index;
                    //filter result and keep only the words that hash % n_server == i
                    if(DEBUG){
                        //print all hascodesof the words
                        for (String key : result.keySet()) {
                            System.out.println(key + " " + abs(key.hashCode() % nServer));
                        }
                    }
                    
                    result = result.entrySet().stream()
                            .filter(entry -> abs(entry.getKey().hashCode() % nServer) == iServer)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    if(DEBUG){
                        System.out.println("N_server: " + n_server + " Server_index: " + server_index);
                        System.out.println("Result: ");
                        //print the map result
                        for (Map.Entry<String, Integer> stringIntegerEntry : result.entrySet()) {
                            System.out.println(stringIntegerEntry.getKey() + " " + stringIntegerEntry.getValue());
                        }
                    }
                    //get the others string from the others server reading filne name_file
                    int n_files = filenames.size();
                    for(int j=0; j<n_files; j++) {
                        String name_file = filenames.get(j);
                        for(int i=0; i<n_server; i++) {
                            if(i == server_index) continue;
                            String wait_file_name = name_file + i;
                            Path filePath = Paths.get(homeDirectory + "/" + wait_file_name);
                            System.out.println("Waiting for file " + filePath);
                            while(!Files.exists(filePath));
                         // Serialize the map
                            try {
                                //read all the file
                                String file = new String(Files.readAllBytes(filePath));
                                deserializeMap(file, result);

                            } catch (FileNotFoundException ex) {
                                    throw new RuntimeException(ex);
                            } catch (IOException ex) {
                                    throw new RuntimeException(ex);
                            }
                        }
                    }
                    if(DEBUG){
                        System.out.println("Result: ");
                        for (Map.Entry<String, Integer> stringIntegerEntry : result.entrySet()) {
                            System.out.println(stringIntegerEntry.getKey() + " " + stringIntegerEntry.getValue());
                        }
                    }
                    socketClient.sendMsgToServer(clientip, clientport, "SHUFFLE FINISHED");
                    System.out.println("Shuffle finished!");
                    
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
                    ips = new String[n_server];
                    filenames = new ArrayList<>();
                    n_server = 0;
                    result = new HashMap<>();

                }else if(line.contains(IP_MSG_IP)) {
                    System.out.println("IPS message received ip");
                    String ipLine = line.split(" ")[1];
                    System.out.println("Server " + n_server + ": " + ipLine);
                    ips[n_server] = ipLine;
                    n_server++;
                    if (n_server == ips.length) {
                        ftpClient = new MyFTPClient(ips);
                        clientip = socketOfServer.getInetAddress().getHostAddress();
                        if(DEBUG){
                            System.out.println("Client IP: " + clientip);
                        }
                        if(DEBUG){
                            System.out.println("IPs: ");
                            for (String ip : ips) {
                                System.out.println(ip);
                            }
                        }
                    }
                    
                }else if(line.contains(GROUP_MSG)) {
                    System.out.println("GROUP message received");
                    //get the ranges GRPOUP fmin fmax
                    String[] range_s = line.split(" ");
                    ranges = new int[range_s.length-1];
                    for(int i=1; i<range_s.length; i++) {
                        ranges[i-1] = Integer.parseInt(range_s[i]);
                    }
                    low_range = ranges[server_index];
                    high_range = ranges[server_index+1];

                    System.out.println("Mine frequencies between " + low_range + " and " + high_range);

                    //do the shuffle, keep words with frequency between low_range and high_range and send the others to the other servers
                    //serialize the map
                    
                    String name_file = filename.split("/")[filename.split("/").length-1];
                    name_file = name_file.split("_")[0];
                    name_file += ("_shuffle_2") ;
                    
                    ftpClient.saveResultsOnServers_range(name_file+server_index, result, server_index, ranges);


                    final int flow_range = low_range;
                    final int fhigh_range = high_range;
                    result = result.entrySet().stream()
                            .filter(entry -> entry.getValue() >= flow_range && entry.getValue() < fhigh_range)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    //wait for the other files
                    for(int i=0; i<n_server; i++) {
                        if(i == server_index) continue;
                        String wait_file_name = name_file + i;
                        Path filePath = Paths.get(homeDirectory + "/" + wait_file_name);
                        System.out.println("Waiting for file " + filePath.toString());
                        while(!Files.exists(filePath));
                        // Serialize the map
                        try {
                            //read all the file
                            String file = new String(Files.readAllBytes(filePath));
                            deserializeMap(file, result);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    //send shuffle2 finished to the client
                    socketClient.sendMsgToServer(clientip, clientport, "SHUFFLE2 FINISHED");
                }else if(line.equals(REDUCE2_MSG)) {
                    System.out.println("REDUCE2 message received");

                    Map<String, Integer> sortedResult = result.entrySet()
                        .stream()
                        .sorted(Comparator.comparing((Map.Entry<String, Integer> entry) -> entry.getValue())
                                .thenComparing(Map.Entry::getKey))
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (e1, e2) -> e1,
                                LinkedHashMap::new));
                    //format like key value\n
                    String result_string = sortedResult.entrySet().stream()
                            .map(entry -> entry.getKey() + " " + entry.getValue() + "\n")
                            .collect(Collectors.joining());
                    
                    //send the result to the client
                    socketClient.sendMsgToServerOrder(clientip, clientport, "FINISHED\n"+result_string);

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
    private static void my_map(String filename, Map<String, Integer> result) {
        //update (append) the result with the words in the file
        try {
            // Read the file
            Stream<String> lines = Files.lines(Paths.get(filename));
            lines.forEach(line -> {
                // Split the line into words
                String[] words = line.split(" ");
                for (String word : words) {
                    // If exists, add the new value to the existing value
                    if (result.containsKey(word)) {
                        result.put(word, result.get(word) + 1);
                    } else {
                        // If doesn't exist, just put the new value
                        result.put(word, 1);
                    }
                }
            });
            lines.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
