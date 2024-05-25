package rs;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.*;

public class MyFTPClient {
    private final static String username = "toto";
    private final static String password = "tata";

    private FTPClient ftpClient = null;
    public MyFTPClient() {
        ftpClient = new FTPClient();
    }

    //save your part of the file on the server, taking into account the number of servers and the server number
    public void saveFileOnServer( String server, int port, String filename, String content, long nServer, int i) {
        try {
            ftpClient.connect(server, port);
            ftpClient.login(username, password);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            // Code to save file, take just the bytes you take care of (i.e. the i-th part)
            ByteArrayInputStream inputStream = new ByteArrayInputStream(content.getBytes());

            System.out.println("Uploading file to server " + server + " part " + i + " of " + nServer);
            ftpClient.storeFile(filename + "_" + i + "_" + nServer, inputStream);
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
