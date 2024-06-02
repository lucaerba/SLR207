package rs;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Master {
    private final static String SERVER_LIST = "server_list.txt";
    private static final String MAP_MSG = "MAP";

    private static MyFTPClient ftpClient = null;
    public static void main(String[] args) {

        String filename = "bonjour.txt";
        List<Map.Entry<String, Integer>> results = null;
        ftpClient = new MyFTPClient();
        try {
            boolean fileExists = false;
            
            //fileExists = checkFileExists(ftpClient, filename);

            if (!fileExists) {
                System.out.println("File does not exist. Creating file.");

                String content = "car cat dog car cat dog car cat dog";

                saveFile(ftpClient, "bonjour.txt", content);
            }

            //send ip addresses to the others
            //nserver
            send_ips();

            //socket to start mapping process
            ask_for_mapping(filename);

            //wait for results
            wait_for_mapping(results);

            ask_for_reduce();

            wait_for_reduce();


            // Code to retrieve and display file content
            //System.out.println("File exists. Displaying file content.");
           
            //print results
            for (Map.Entry<String, Integer> entry : results) {
                System.out.println(entry.getKey() + " " + entry.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void wait_for_reduce() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'wait_for_reduce'");
    }

    private static void ask_for_reduce() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'ask_for_reduce'");
    }

    private static void send_ips(){
        //send to all the servers, the numbers of servers, ips+port of the other servers
        try {
            BufferedReader reader = new BufferedReader(new FileReader(SERVER_LIST));
            List<String> lines = reader.lines().collect(Collectors.toList());
            int n_server = lines.size();
            System.out.println("n_server: " + n_server);
            String line = null;
            MySocketClient socket = new MySocketClient();

            for (int i = 0; i < n_server; i++) {
                line = lines.get(i);
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]);
                System.out.println("Sending ips to server " + server + " on port " + port);

                //create a socket to connect to the server
                socket.sendMsgToServer(server, port, "IPSN " + n_server);
                for(int j=0; j<n_server; j++) {
                    if(j != i) {
                        String msg = "IPSI " + lines.get(j);
                        socket.sendMsgToServer(server, port, msg);
                    }
                }
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
    //list the servers, connect to the socket, (port + 100) and ask to do mapping
    private static void ask_for_mapping( String filename) {
        try {

            BufferedReader reader = new BufferedReader(new FileReader(SERVER_LIST));
            List<String> lines = reader.lines().collect(Collectors.toList());
            int n_server = lines.size();
            System.out.println("n_server: " + n_server);
            String line = null;
            MySocketClient socket = new MySocketClient();

            for (int i = 0; i < n_server; i++) {
                line = lines.get(i);
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]);
                System.out.println("Asking server " + server + " to do mapping on port " + port);

                String file_part = filename + "_" + i + "_" + n_server;
                //create a socket to connect to the server
                socket.sendMsgToServer(server, port, MAP_MSG + " " + file_part);
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void wait_for_mapping(List<Map.Entry<String, Integer>> results) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(SERVER_LIST));
            List<String> lines = reader.lines().collect(Collectors.toList());
            int n_server = lines.size();
            System.out.println("n_server: " + n_server);
            String line = null;
            MySocketClient socket = null;
            boolean first = true;
            for (int i = 0; i < n_server; i++) {
                line = lines.get(i);
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]);
                int mapping_port = port;
                System.out.println("Asking server " + server + " for results");
                socket = new MySocketClient();
                socket.sendMsgToServer(server, mapping_port, "REDUCE");

                String response = socket.receiveMsgFromServer(server, mapping_port);
                while(response.compareTo("END") != 0) {
                    System.out.println("reduce result: " + response);
                    if(first) {
                        results = new ArrayList<Map.Entry<String, Integer>>();
                        first = false;
                    }
                    response = reader.readLine();
                    String[] parts2 = response.split(" ");
                    results.add(Map.entry(parts2[0], Integer.parseInt(parts2[1])));
                }
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    

    //save the file splitting the file into n parts and saving them into the n servers on the list
    private static void saveFile(MyFTPClient ftpClient, String filename, String content){
        try {
            BufferedReader reader = new BufferedReader(new FileReader(SERVER_LIST));
            List<String> lines = reader.lines().collect(Collectors.toList());
            reader.close();
            int n_server = lines.size();
            System.out.println("n_server: " + n_server);

            for (int i = 0; i < n_server; i++) {
                String line = lines.get(i);
                System.out.println("line: " + line);
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]);
                port += 100;
                //take just the content we are responsible for
                String content_part = content.substring(i*content.length()/n_server, (i+1)*content.length()/n_server);
                System.out.println("saving " + content_part);
                ftpClient.saveFileOnServer(server, port, filename, content_part, n_server, i);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //retrieve the file from the servers and merge them
    private static InputStream retrieveFile(MyFTPClient ftpClient, String filename){
        String content = "";
        try {
            BufferedReader reader = new BufferedReader(new FileReader(SERVER_LIST));
            List<String> lines = reader.lines().collect(Collectors.toList());
            int n_server = lines.size();

            for (int i = 0; i < n_server; i++) {
                String line = lines.get(i);
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]);
                String part_filename = filename + "_" + i + "_" + n_server;
                content += ftpClient.retrieveFileFromServer(server, port, part_filename);
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ByteArrayInputStream(content.getBytes());
    }

    private static boolean checkFileExists(MyFTPClient ftpClient, String filename){
        //open SERVER_LIST and check if at least one server has the file
        //if yes, return true
        //if no, return false
        try {
            BufferedReader reader = new BufferedReader(new FileReader(SERVER_LIST));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]);
                if (ftpClient.checkFileExistsOnServer(server, port, filename)) {
                    reader.close();
                    return true;
                }
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

}
