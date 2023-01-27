package org.gox.tcpproxy;

import org.gox.tcpproxy.utils.HexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class PgProxy {

    private static final Logger logger = LoggerFactory.getLogger(PgProxy.class);

    private final ServerSocket serverSocket;
    private final String destinationHostname;
    private final int destinationPort;

    public PgProxy(int port, String destinationHostname, int destinationPort) throws IOException {
        this.destinationPort = destinationPort;
        this.destinationHostname = destinationHostname;
        this.serverSocket = new ServerSocket(port);
        logger.info("Listening new client connection on {}", port);
    }

    public void start() {
        boolean isInterrupted = false;
        Socket destinationSocket;
        while(!isInterrupted) {
            try {
                Socket clientSocket = serverSocket.accept();
                DataInputStream sourceIn = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream sourceOut = new DataOutputStream(clientSocket.getOutputStream());
                logger.info("New client connected");

                destinationSocket = new Socket(destinationHostname, destinationPort);
                DataInputStream destinationIn = new DataInputStream(destinationSocket.getInputStream());
                DataOutputStream destinationOut = new DataOutputStream(destinationSocket.getOutputStream());
                logger.info("Connected to destination {}:{}.", destinationHostname, destinationPort);

                new Thread(buildTcpRunnable("Source->Destination", sourceIn, destinationOut)).start();
                new Thread(buildTcpRunnable("Destination->Source", destinationIn, sourceOut)).start();

                logger.info("Redirecting TCP flow : {}:{} <-> {}:{}", clientSocket.getInetAddress(), clientSocket.getPort(), destinationHostname, destinationPort);
            } catch (IOException e) {
                logger.error("Fail to connect to socket", e);
                isInterrupted = true;
            }
        }
    }

    private Runnable buildTcpRunnable(String name, DataInputStream in, DataOutputStream out) {
        return () -> {
            boolean isInterrupted = false;
            while (!isInterrupted) {
                try {
                    int available = in.available();
                    if (available > 0) {
                        byte[] buffer = new byte[available];
                        int nbBytes = in.read(buffer);
                        String hexPayload = HexUtils.bytesToHex(buffer);
                        String utf8Payload = new String(buffer, StandardCharsets.UTF_8).replaceAll("\0", ".");
                        logger.info("{} ({} bytes) HEX: {} UTF-8: {}", name, nbBytes, hexPayload, utf8Payload);
                        out.write(buffer);
                    }
                } catch (IOException e){
                    logger.error("Fail to read from socket", e);
                    isInterrupted = true;
                }
            }
        };
    }

    public static void main(String[] args) throws IOException {
        new PgProxy(5666, "localhost", 5432).start();
    }
}
