import java.io.*;
import java.net.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class HiveServer {

    ArrayList<PrintWriter> clientOutputStreams;
    
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private ResultSet res;
    ResultSetMetaData metaData;
    
    private String sql;
    private Statement statement;
    private Connection con;
    private String delimiter =" ";
    private String rowline;
    
    public class ClientHandler implements Runnable{

        BufferedReader reader;
        Socket sock;

        //constructor
        public ClientHandler(Socket clientSocket){
        	
            try{
                sock = clientSocket;
                InputStreamReader isReader = new InputStreamReader(sock.getInputStream());
                reader = new BufferedReader(isReader);
            }catch(Exception ex){
                ex.printStackTrace();
            }
            
            try {
      	      Class.forName(driverName);
     	      
      	      try {
      	    	  con = DriverManager.getConnection("jdbc:hive://10.0.3.23:10000/default", "hive", "ujgs");
      	    	  statement = con.createStatement();
      	      		} catch (SQLException e) {
      	      			e.printStackTrace();
      	      	  }
      	    } catch (ClassNotFoundException e) {
      	      e.printStackTrace();
      	      System.exit(1);
      	    }
            
        }

        public void run(){
            String message;
            try{
               
                while((message = reader.readLine()) != null){
                    System.out.println("Querry: " + message);
                   
                    try{  
                    	    // regular hive query
                    	    sql = message;
                    	    System.out.println("Running: " + sql);
                    	    res = statement.executeQuery(sql);
                    	    metaData = res.getMetaData();
                    	    int numberOfColumns = metaData.getColumnCount();
                    	    
                    	    while (res.next()) {
                    	    	for (int i =1; i<= numberOfColumns; i++){
                    	    		rowline = res.getString(i) + delimiter;
                    	    		//System.out.println(rowline);
                    	    		sendMessage(rowline);
                    	    	}
                    	    	sendMessage("\n");
                    	    }      
                    	}catch(Exception e){
                    	System.out.println("Running querry exception: " + e);
                    	sendMessage("UNKNOWN OUTPUT PLEASE CHECK QUERY" + e);
                    	e.printStackTrace();}
                    //sendMessage(message);
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }

    }


    public void go(){
     
     clientOutputStreams = new ArrayList<PrintWriter>();

     try{
         ServerSocket serverSock = new ServerSocket(5000);

         while(true){
             Socket clientSocket = serverSock.accept();
             PrintWriter writer = new PrintWriter(clientSocket.getOutputStream());
             clientOutputStreams.add(writer);
             //    writer.println(clientOutputStreams);
             
             Thread t = new Thread(new ClientHandler(clientSocket));
             t.start();
             System.out.println("Got a connection!");
         }

     }catch(Exception exx){
         exx.printStackTrace();
     }
  }

     public void sendMessage(String messages){
     
     Iterator<PrintWriter> it = clientOutputStreams.iterator();
     
     while(it.hasNext()){
         try{

             PrintWriter writer = (PrintWriter) it.next();
             writer.print(messages);
             writer.flush();
         }catch(Exception e){
             e.printStackTrace();
         }
     }
  }

     public static void main(String[] args){
 
         new HiveServer().go();
     }

}