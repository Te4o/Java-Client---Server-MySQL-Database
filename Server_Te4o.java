import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 *
 * @author Te4o
 */
public class Server_Te4o {
    public static void main(String[] args) throws IOException, InterruptedException {
        // TODO code application logic here
        ServerSocket serverSocket = new ServerSocket(7009);
        
        while(true){
            System.out.println("Stani be :D");
            Socket socket = serverSocket.accept(); 
                Thread tr = new Thread(new SQLServer_Te4o(socket));
                tr.start();
            }
        }
}