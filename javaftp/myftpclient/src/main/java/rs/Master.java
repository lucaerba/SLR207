package rs;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.net.InetAddress;
import java.net.ServerSocket;

public class Master {
    private final static String SERVER_LIST = "server_list.txt";
    private static final String MAP_MSG = "MAP";
    private static final String SHUFFLE_MSG = "SHUFFLE";

    private static MyFTPClient ftpClient = null;

    public static void main(String[] args) {
        for(int i=2; i<14; i+=1){
            for(int j=1; j<8; j+=1){

                System.out.println("---- n_server: " + i + " n_files: " + j+ " ----");
               //create a timer to mesure the execution time and the different subtimes
                long time, time_before, startTime, splitTime, ipsTime, map1Time, map2Time, reduceTime, shuffleTime, shuffleTime1, reduce2Time, endTime;
                startTime = System.currentTimeMillis();
                int n_server = i;
                List<String> lines = null;
                int n_files = j;
                //int n_files = args.length > 0 ? Integer.parseInt(args[0]) : 1;

                //just read n_machines from the server_list
                try (BufferedReader reader = new BufferedReader(new FileReader(SERVER_LIST))) {
                    lines = reader.lines().collect(Collectors.toList());
                    lines = lines.subList(0, n_server);

                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                //start the timer
                

                String dirPath = "/cal/commoncrawl";
                List<Map.Entry<String, Integer>> results = null;
                ftpClient = new MyFTPClient();
                try {
                    //boolean filesExist = checkFileExists(ftpClient, dirPath);
                    boolean filesExist = false;
                    String[] filenames = new String[n_files];
                    if (!filesExist) {
                        System.out.println("Files do not exist. Creating and splitting files.");

                        readAllFilesInDirectory( n_server, lines, dirPath, n_files, filenames);
                        
                    }
                    splitTime = System.currentTimeMillis()-startTime;
                    System.out.println("file split: " + splitTime + "ms");

                    //send ip addresses to the others
                    //nserver
                    time_before = System.currentTimeMillis();
                    send_ips(n_server, lines);
                    time = System.currentTimeMillis();
                    ipsTime = time - time_before;
                    System.out.println("ips sent: " + ipsTime + "ms");

                    time_before = System.currentTimeMillis();
                    //socket to start mapping process
                    ask_for_mapping(n_server, lines, filenames);

                    //wait for results
                    wait_for_mapping(n_server, filenames);
                    time = System.currentTimeMillis();
                    map1Time = time - time_before;
                    System.out.println("map1 done: " + map1Time + "ms");

                    time_before = System.currentTimeMillis();
                    ask_for_shuffle(n_server, lines);
                    //wait for results
                    wait_for_shuffle(n_server, lines, "SHUFFLE FINISHED");

                    time = System.currentTimeMillis();
                    shuffleTime1 = time - time_before;
                    System.out.println("shuffle1 done: " + shuffleTime1 + "ms");

                    time_before = System.currentTimeMillis();
                    ask_for_reduce(n_server, lines);

                    wait_for_reduce_send_groups(n_server, lines);
                    time = System.currentTimeMillis();
                    reduceTime = time - time_before;
                    System.out.println("reduce done: " + reduceTime + "ms");

                    time_before = System.currentTimeMillis();
                    wait_for_shuffle(n_server, lines, "SHUFFLE2 FINISHED");
                    time = System.currentTimeMillis();
                    shuffleTime = time - time_before;
                    System.out.println("shuffle2 done: " + shuffleTime + "ms");

                    time_before = System.currentTimeMillis();
                    ask_for_reduce2(n_server, lines);
                    time = System.currentTimeMillis();
                    reduce2Time = time - time_before;
                    System.out.println("reduce2 done: " + reduce2Time + "ms");

                    time_before = System.currentTimeMillis();
                    wait_and_merge(n_server, lines);
                    time = System.currentTimeMillis();
                    endTime = time - time_before;
                    System.out.println("merge done: " + endTime + "ms");

                    // Code to retrieve and display file content
                    //System.out.println("File exists. Displaying file content.");
                    //append to the time file the times
                    try {
                        FileWriter writer = new FileWriter("times.txt", true);
                        writer.write("---- n_server: " + n_server +" n_files: " + n_files +" ----\n");
                        writer.write("split: " + splitTime + "\n");
                        writer.write("ips: " + ipsTime + "\n");
                        writer.write("map1: " + map1Time + "\n");
                        writer.write("shuffle1: " + shuffleTime1 + "\n");
                        writer.write("reduce: " + reduceTime + "\n");
                        writer.write("shuffle: " + shuffleTime + "\n");
                        writer.write("reduce2: " + reduce2Time + "\n");
                        writer.write("merge: " + endTime + "\n");
                        writer.write("total time: " + (endTime + reduce2Time + shuffleTime + reduceTime + map1Time + ipsTime + splitTime + shuffleTime1) + "\n");
                        writer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        
    }

    private static void wait_and_merge(int nServer, List<String> ips) {
        //wait for all the results to arrive and merge them into a single list to create a file with that
        try {
            System.out.println("n_server: " + nServer);
 
            ServerSocket socket = new ServerSocket(1234);
            File file = new File("output.txt");
            FileWriter writer = new FileWriter(file, false);

            for (int i = 0; i < nServer; i++) {
                boolean processed = false;
                while (!processed) {
                    Socket clientSocket = socket.accept();

                    System.out.println("clientIp: " + clientSocket.getInetAddress().getHostName());
                    //clientIp: tp-1d23-21.enst.fr , just keep until the first dot
                    String clientIp = clientSocket.getInetAddress().getHostName().split("\\.")[0];
                    String expectedIp = ips.get(i).split(":")[0];

                    // Check if the IP matches the expected order
                    if (clientIp.equals(expectedIp)) {

                        BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

                        System.out.println("Processing messages from server: " + clientIp);
                        //send to socket OK
                        BufferedWriter writer2 = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
                        writer2.write("OK");
                        writer2.newLine();
                        writer2.flush();

                        String msg;
                        while ((msg = reader.readLine()) != null) {
                            
                            System.out.println("msg: " + msg);
                            if (msg.contains("FINISHED")) {
                                System.out.println("Server " + clientIp + " finished reduce2");
                                processed = true;
                                while((msg=reader.readLine()) != null ){
                                    //System.out.println("msg: " + msg);
                                    writer.write(msg);
                                    if(!msg.equals("\n")){
                                        writer.write("\n");
                                    }
                                }
                                writer.flush();
                                break;
                            }
                        }
                        reader.close();
                        clientSocket.close();
                        writer2.close();
                    } else {
                        System.out.println("Unexpected server: " + clientIp + ", expected: " + expectedIp);
                        BufferedWriter writer2 = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
                        writer2.write("KO");
                        writer2.close();
                        clientSocket.close();
                    }
                }
            }

            socket.close();
            writer.close();
            file.createNewFile();
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

    private static void wait_for_shuffle(int n_server, List<String> lines, String MSG) {
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
                    if (msg.contains(MSG)) {
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
        //got the range of fmin and fmax, divide the range into n_server groups {fmin, n_server-1 groups} and send all the groups to all the machines
        int[] ranges = new int[n_server+1];
        ranges[0] = fmin_min;
        ranges[1] = fmin_min+1;
        ranges[n_server] = fmax_max+1;
        int range = (fmax_max - fmin_min - 2) / n_server;
        for (int i = 2; i < n_server; i++) {
            ranges[i] = ranges[i-1] + range ;
        }
        String msg = "GROUPS ";
        System.out.println("Ranges: ");
        for (int j = 0; j < n_server+1; j++) {
            msg += ranges[j] + " ";
            System.out.println(ranges[j]);
        }
        try {
            for (int i = 0; i < n_server; i++) {
                String line = lines.get(i);
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]);
                MySocketClient send = new MySocketClient();
                send.sendMsgToServer(server, port, msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
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
                    String msg = "IPSI " + lines.get(j);
                    socket.sendMsgToServer(server, port, msg);

                }
            }
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
    //list the servers, connect to the socket, (port + 100) and ask to do mapping
    private static void ask_for_mapping( int n_server, List<String> lines, String[] files) {
        try {
            System.out.println("n_server: " + n_server);
            String line = null;
            MySocketClient socket = new MySocketClient();
            for(String file : files){
                for (int i = 0; i < n_server; i++) {
                    line = lines.get(i);
                    String[] parts = line.split(":");
                    String server = parts[0];
                    int port = Integer.parseInt(parts[1]);
                    System.out.println("Asking server " + server + " to do mapping on port " + port);

                    //create a socket to connect to the server
                    socket.sendMsgToServer(server, port, MAP_MSG + " " + file);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void ask_for_shuffle( int n_server, List<String> lines) {
        try {
            System.out.println("n_server: " + n_server);
            String line = null;
            MySocketClient socket = new MySocketClient();

            for (int i = 0; i < n_server; i++) {
                line = lines.get(i);
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]);
                System.out.println("Asking server " + server + " to do shuffle on port " + port);

                //create a socket to connect to the server
                socket.sendMsgToServer(server, port, SHUFFLE_MSG );
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void wait_for_mapping(int n_server, String[] files) {
        try {
            System.out.println("n_server: " + n_server);
            ServerSocket socket = new ServerSocket(1234);

            for (int i = 0; i < n_server*files.length; i++) {
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

    private static void readAllFilesInDirectory(int n_server, List<String> lines, String dirPath, int n_files, String[] files) throws IOException {
        int fileCounter = 0;

        try (Stream<Path> paths = Files.walk(Paths.get(dirPath))) {
            // Filter only regular files that end with .warc.wet
            for (Path p : (Iterable<Path>) paths.filter(Files::isRegularFile).filter(p -> p.toString().endsWith(".warc.wet"))::iterator) {
                if (fileCounter >= n_files) {
                    break;
                }

                System.out.println("Processing file: " + p);
                StringBuilder[] contents = new StringBuilder[n_server];
                for (int i = 0; i < n_server; i++) {
                    contents[i] = new StringBuilder();
                }

                try (BufferedReader reader = Files.newBufferedReader(p)) {
                    String line;
                    int lineNumber = 0;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        line = line.replaceAll("\\s+", " ");
                        line = line.replaceAll("\n", " ");
                        // Distribute each line to a different server
                        contents[lineNumber % n_server].append(line).append(" ");
                        lineNumber++;
                    }
                } catch (IOException e) {
                    System.out.println("Error reading file: " + p);
                    e.printStackTrace();
                    continue; // Skip to the next file if an error occurs
                }

                String[] filenames = p.getFileName().toString().split("/");
                String filename = filenames[filenames.length - 1];
                files[fileCounter] = filename;

                // Save the distributed content to the servers
                saveFile(n_server, lines, filename, contents);

                fileCounter++;
            }
        }
    }

    private static void saveFile(int n_server, List<String> lines, String filename, StringBuilder[] contents) {
        try {
            System.out.println("n_server: " + n_server);

            for (int i = 0; i < n_server; i++) {
                String line = lines.get(i);
                String[] parts = line.split(":");
                String server = parts[0];
                int port = Integer.parseInt(parts[1]) + 100;

                // Save the content chunk on the server
                MyFTPClient ftpClient = new MyFTPClient(); // Assuming you have an FTP client class
                ftpClient.saveFileOnServer(server, port, filename, contents[i].toString(), n_server, i);
            }
        } catch (Exception e) {
            System.out.println("Error saving file");
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
