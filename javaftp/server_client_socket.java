import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class SimpleServerProgram {

   public static void main(String args[]) {

       ServerSocket listener = null;
       String line;
       BufferedReader is;
       BufferedWriter os;
       Socket socketOfServer = null;

       // Try to open a server socket on port 9999
       // Note that we can't choose a port less than 1023 if we are not
       // privileged users (root)

 
       try {
           listener = new ServerSocket(9999);
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
               
               // Write to socket of Server
               // (Send to client)
               os.write(">> " + line);
               // End of line
               os.newLine();
               // Flush data.
               os.flush();  


               // If users send QUIT (To end conversation).
               if (line.equals("QUIT")) {
                   os.write(">> OK");
                   os.newLine();
                   os.flush();
                   break;
               }
           }

       } catch (IOException e) {
           System.out.println(e);
           e.printStackTrace();
       }
       System.out.println("Sever stopped!");
   }
}


import java.io.*;
import java.net.*;

public class SimpleClient {

   public static void main(String[] args) {

       // Server Host
       final String serverHost = "tp-1a226-01.enst.fr";

       Socket socketOfClient = null;
       BufferedWriter os = null;
       BufferedReader is = null;

       try {
           
           // Send a request to connect to the server is listening
           // on machine 'localhost' port 9999.
           socketOfClient = new Socket(serverHost, 9999);

           // Create output stream at the client (to send data to the server)
           os = new BufferedWriter(new OutputStreamWriter(socketOfClient.getOutputStream()));


           // Input stream at Client (Receive data from the server).
           is = new BufferedReader(new InputStreamReader(socketOfClient.getInputStream()));

       } catch (UnknownHostException e) {
           System.err.println("Don't know about host " + serverHost);
           return;
       } catch (IOException e) {
           System.err.println("Couldn't get I/O for the connection to " + serverHost);
           return;
       }

       try {

           // Write data to the output stream of the Client Socket.
           os.write("HELO");
 
           // End of line
           os.newLine();
   
           // Flush data.
           os.flush();  
           os.write("I am Tom Cat");
           os.newLine();
           os.flush();
           os.write("QUIT");
           os.newLine();
           os.flush();


           
           // Read data sent from the server.
           // By reading the input stream of the Client Socket.
           String responseLine;
           while ((responseLine = is.readLine()) != null) {
               System.out.println("Server: " + responseLine);
               if (responseLine.indexOf("OK") != -1) {
                   break;
               }
           }

           os.close();
           is.close();
           socketOfClient.close();
       } catch (UnknownHostException e) {
           System.err.println("Trying to connect to unknown host: " + e);
       } catch (IOException e) {
           System.err.println("IOException:  " + e);
       }
   }

}