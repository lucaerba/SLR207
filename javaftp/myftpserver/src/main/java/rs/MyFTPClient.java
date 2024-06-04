package rs;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.*;
import java.util.stream.Stream;

public class MyFTPClient {
    private final static String username = "toto";
    private final static String password = "tata";
    private String[] ips;
    private FTPClient ftpClient = null;
    
    public MyFTPClient(String[] ips) {
        ftpClient = new FTPClient();
        this.ips = ips;
    }

    public void saveResultsOnServers(String filename, String content) {
        int nServer = ips.length;
        for (int i = 0; i < nServer; i++) {
            //split ip port
            String[] ipPort = ips[i].split(":");
            //, send just the words with the hash % n_server == i
            int finalI = i;
            content = Stream.of(content.split(" "))
                    .filter(word -> Math.abs(word.hashCode()) % nServer == finalI)
                    .reduce("", (acc, word) -> acc + " " + word);
            saveFileOnServer(ipPort[0], Integer.parseInt(ipPort[1])+100, filename, content);
        }
    }
    //save your part of the file on the server, taking into account the number of servers and the server number
    public void saveFileOnServer( String server, int port, String filename, String content) {
        try {
            ftpClient.connect(server, port);
            ftpClient.login(username, password);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            // Code to save file, take just the bytes you take care of (i.e. the i-th part)
            ByteArrayInputStream inputStream = new ByteArrayInputStream(content.getBytes());

            System.out.println("Uploading file to server " + server);
            ftpClient.storeFile(filename , inputStream);
            int errorCode = ftpClient.getReplyCode();

            if (errorCode != 226) {
                System.out.println("File upload failed. FTP Error code: " + errorCode);
            } else {
                System.out.println("File uploaded successfully.");
            }

            ftpClient.logout();
            ftpClient.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String retrieveFileFromServer(String server, int port, String filename) {
        String content = "";
        try {
            ftpClient.connect(server, port);
            ftpClient.login(username, password);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            // Code to retrieve and display file content
            InputStream inputStream = ftpClient.retrieveFileStream(filename);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                content += line;
            }

            reader.close();
            ftpClient.completePendingCommand();
            ftpClient.logout();
            ftpClient.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return content;
    }

    public boolean checkFileExistsOnServer(String server, int port, String filename){
        try {
            ftpClient.connect(server, port);
            ftpClient.login(username, password);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            // Code to display files
            FTPFile[] files = ftpClient.listFiles();
            boolean fileExists = false;
            for (FTPFile file : files) {
                if (file.getName().contains(filename)) {
                    fileExists = true;
                    break;
                }
            }
            return fileExists;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
