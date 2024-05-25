package rs;

import java.io.*;
import java.net.Socket;

public class MySocketClient {
    //create a class implementing the client side of the socket, to send, receive and display messages
    //from the server
    //use the following methods:
    //1. sendMsgToServer(String server, int port, String msg) - to send a message to the server
    //2. receiveMsgFromServer(String server, int port) - to receive a message from the server
    //3. displayMsg(String msg) - to display a message

    //constructor
    public MySocketClient() {

    }

    //method to send a message to the server
    public void sendMsgToServer(String server, int port, String msg) {
        //create a socket to connect to the server

            try (Socket socket = new Socket(server, port)) {
                //create a buffered writer to write to the server
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                //write the message to the server
                writer.write(msg);
                writer.newLine();
                writer.flush();
                //close the writer
                writer.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }

    }

    //method to receive a message from the server
    public String receiveMsgFromServer(String server, int port) {
        //create a socket to connect to the server
        Socket socket = null;
        try {
            socket = new Socket(server, port);
            //create a buffered reader to read from the server
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            //read and return the message from the server
            String msg = reader.readLine();
            //close the reader
            reader.close();

            return msg;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    //method to display a message
    public void displayMsg(String msg) {
        //display the message
    }

}
