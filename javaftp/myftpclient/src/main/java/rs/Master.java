package rs;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.net.ServerSocket;

public class Master {
    private final static String SERVER_LIST = "server_list.txt";
    private static final String MAP_MSG = "MAP";

    private static MyFTPClient ftpClient = null;

    public static void main(String[] args) {
        //create a timer to mesure the execution time and the different subtimes
        long startTime, splitTime, ipsTime, map1Time, map2Time, reduceTime, shuffleTime, reduce2Time, endTime;
        startTime = System.currentTimeMillis();
        int n_server = 0;
        List<String> lines = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(SERVER_LIST))) {
            lines = reader.lines().collect(Collectors.toList());
            n_server = lines.size();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        //start the timer
        String filename = "bonjour.txt";
        List<Map.Entry<String, Integer>> results = null;

        ftpClient = new MyFTPClient();
        try {
            boolean fileExists = false;
            
            //fileExists = checkFileExists(ftpClient, filename);

            if (!fileExists) {
                System.out.println("File does not exist. Creating file.");

                String content = "dog cat car tree house bike cat dog car tree car cat dog bike tree house car cat dog tree bike house car dog cat bike tree house cat dog car tree bike house cat dog car tree bike house cat dog car tree bike house";

                saveFile(n_server, lines, ftpClient, filename, content);
            }
            splitTime = System.currentTimeMillis()-startTime;
            System.out.println("file split: " + splitTime + "ms");

            //send ip addresses to the others
            //nserver
            send_ips(n_server, lines);
            ipsTime = System.currentTimeMillis()-startTime-splitTime;
            System.out.println("ips sent: " + ipsTime + "ms");

            //socket to start mapping process
            ask_for_mapping(n_server, lines, filename);

            //wait for results
            wait_for_mapping(n_server);
            map1Time = System.currentTimeMillis()-startTime-ipsTime;
            System.out.println("mapping done: " + map1Time + "ms");

            ask_for_reduce(n_server, lines);

            wait_for_reduce_send_groups(n_server, lines);
            reduceTime = System.currentTimeMillis()-startTime-map1Time;
            System.out.println("reduce done: " + reduceTime + "ms");

            wait_for_shuffle(n_server, lines);

            ask_for_reduce2(n_server, lines);

            wait_and_merge(n_server, lines, results);

            // Code to retrieve and display file content
            //System.out.println("File exists. Displaying file content.");
           

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void wait_and_merge(int nServer, List<String> lines, List<Map.Entry<String, Integer>> results) {
        //wait for all the results to arrive and merge them into a single list to create a file with that
        try {
            System.out.println("n_server: " + nServer);
            String line = null;
            ServerSocket socket = new ServerSocket(1234);

            for (int i = 0; i < nServer; i++) {
                while(true) {
                    Socket s = socket.accept();
                    BufferedReader is = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    String msg = is.readLine();
                    if (msg.contains("FINISHED")) {
                        System.out.println("Server " + s.getInetAddress() + " finished reduce2");
                        break;
                    }
                }
            }

            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void ask_for_reduce2(int n_server, List<String> lines) {
        try {
            System.out.println("n_server: " + n_server);
            String line = null;
            for (int i = 0; i < n_server; i++) {
                line = lines.get(i);
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]);
                System.out.println("Asking server " + server + " to do reduce2");
                MySocketClient send = new MySocketClient();
                send.sendMsgToServer(server, port, "REDUCE2");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void wait_for_shuffle(int n_server, List<String> lines) {
        //wait for the n_server to send the shuffle2 finished
        try {
            System.out.println("n_server: " + n_server);
            String line = null;
            ServerSocket socket = new ServerSocket(1234);

            for (int i = 0; i < n_server; i++) {
                while(true) {
                    Socket s = socket.accept();
                    BufferedReader is = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    String msg = is.readLine();
                    if (msg.contains("SHUFFLE2 FINISHED")) {
                        System.out.println("Server " + s.getInetAddress() + " finished shuffle2");
                        break;
                    }
                }
            }

            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void wait_for_reduce_send_groups(int n_server, List<String> lines) {
        // wait all the fmin fmax from the servers, collect them and save fmin_min and fmax_max //msg format: fmin fmax
        int fmin_min = Integer.MAX_VALUE;
        int fmax_max = Integer.MIN_VALUE;

        ServerSocket socket = null;
        try {
            socket = new ServerSocket(1234);
            for (int i = 0; i < n_server; i++) {
                Socket s = socket.accept();
                BufferedReader is = new BufferedReader(new InputStreamReader(s.getInputStream()));
                String msg = is.readLine();
                String[] parts = msg.split(" ");
                int fmin = Integer.parseInt(parts[0]);
                int fmax = Integer.parseInt(parts[1]);
                if (fmin < fmin_min) {
                    fmin_min = fmin;
                }
                if (fmax > fmax_max) {
                    fmax_max = fmax;
                }
                System.out.println("Server " + s.getInetAddress() + " finished reduce");

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //got the range of fmin and fmax, divide the range into n_server groups
        int range = fmax_max - fmin_min + 1;
        int group_size = range / n_server;
        int group_start = fmin_min;
        int group_end = fmin_min + group_size;
        for (int i = 0; i < n_server; i++) {
            String line = lines.get(i);
            String[] parts = line.split(":");
            String server = parts[0];
            int port = Integer.parseInt(parts[1]);
            System.out.println("Sending group " + i + " to server " + server + " on port " + port);
            MySocketClient send = new MySocketClient();
            send.sendMsgToServer(server, port, "GROUP " + group_start + " " + group_end);
            group_start = group_end;
            group_end = group_start + group_size;
        }
       
    }

    private static void ask_for_reduce(int n_server, List<String> lines) {
        try {
             System.out.println("n_server: " + n_server);
            String line = null;
            for (int i = 0; i < n_server; i++) {
                line = lines.get(i);
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]);
                System.out.println("Asking server " + server + " to do reduce");
                MySocketClient send = new MySocketClient();
                send.sendMsgToServer(server, port, "REDUCE");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void send_ips(int n_server, List<String> lines){
        //send to all the servers, the numbers of servers, ips+port of the other servers
        try {

            System.out.println("n_server: " + n_server);
            String line = null;
            MySocketClient socket = new MySocketClient();

            for (int i = 0; i < n_server; i++) {
                line = lines.get(i);
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]);
                System.out.println("Sending ips to server " + server + " on port " + port);

                //create a socket to connect to the server, send the number of servers

                socket.sendMsgToServer(server, port, "IPSN " + n_server + " " + i );
                for(int j=0; j<n_server; j++) {
                    if(j != i) {
                        String msg = "IPSI " + lines.get(j);
                        socket.sendMsgToServer(server, port, msg);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
    //list the servers, connect to the socket, (port + 100) and ask to do mapping
    private static void ask_for_mapping( int n_server, List<String> lines, String filename) {
        try {
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void wait_for_mapping(int n_server) {
        try {
            System.out.println("n_server: " + n_server);
            String line = null;
            boolean first = true;
            ServerSocket socket = new ServerSocket(1234);

            for (int i = 0; i < n_server; i++) {
                while(true) {
                    Socket s = socket.accept();
                    BufferedReader is = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    String msg = is.readLine();
                    if (msg.contains("FINISHED")) {
                        System.out.println("Server " + s.getInetAddress() + " finished mapping");
                        break;
                    }
                }
            }

            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    

    //save the file splitting the file into n parts and saving them into the n servers on the list
    private static void saveFile(int n_server, List<String> lines, MyFTPClient ftpClient, String filename, String content){
        try {
            System.out.println("n_server: " + n_server);

            for (int i = 0; i < n_server; i++) {
                String line = lines.get(i);
                System.out.println("line: " + line);
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]);
                port += 100;
                //take just the content we are responsible for taking all the i+n words
                String content_part = "";
                String[] words = content.split(" ");
                for (int j = i; j < words.length; j += n_server) {
                    content_part += words[j] + " ";
                }
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
