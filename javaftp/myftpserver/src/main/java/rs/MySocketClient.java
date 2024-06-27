package rs;

import java.io.*;
import java.net.Socket;
import java.net.ConnectException;

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
        boolean connected = false;
        Socket socket = null;

        while (!connected) {
            try {
                socket = new Socket(server, port);
                if (socket.isConnected()) {
                   
                    // Use try-with-resources to ensure writer is closed properly
                    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {
                        writer.write(msg);
                        writer.newLine();
                        writer.flush();
                    }

                    connected = true; // Message sent successfully
                }
            } catch (ConnectException e) {
                System.out.println("Connection refused. Retrying...");
                try {
                    Thread.sleep(1000); // Wait 1 second before retrying
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    System.out.println("Retry interrupted");
                    return;
                }
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
                connected = false;
            }  finally {
                // Ensure the socket is closed properly
                if (socket != null && !socket.isClosed()) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        System.out.println("Error closing socket: " + e.getMessage());
                    }
                }
            }
        }
    }

    public void sendMsgToServerOrder(String server, int port, String msg) {
        boolean connected = false;
        Socket socket = null;

        while (!connected) {
            try {
                socket = new Socket(server, port);
                if (socket.isConnected()) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String ok = reader.readLine();
                    while (ok == null) {
                        ok = reader.readLine();
                    }
                    System.out.println("Received: " + ok);
                    if ("OK".equals(ok)) {
                        System.out.println("Server ready. Sending message...");
                        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {
                            writer.write(msg);
                            writer.newLine();
                            writer.flush();
                        }
                        connected = true; // Message sent successfully
                        System.out.println("Message sent successfully");
                        socket.close();
                    }else {
                        socket.close();
                        System.out.println("Server not ready. Retrying...");
                        Thread.sleep(200);
                    }
                }else {
                    socket.close();
                    System.out.println("Server not ready. Retrying...");
                    Thread.sleep(200);
                }
            } catch (ConnectException e) {
                System.out.println("Connection refused. Retrying...");
                try {
                    Thread.sleep(1000); // Wait 1 second before retrying
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    System.out.println("Retry interrupted");
                    return;
                }
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
                connected = false;
            } catch (InterruptedException e) {
                System.out.println("Error: " + e.getMessage());
                throw new RuntimeException(e);
            }

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
