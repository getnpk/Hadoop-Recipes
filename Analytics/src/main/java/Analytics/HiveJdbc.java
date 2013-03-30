package Analytics;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbc {
	
  private static final String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
  
  Connection connection;
  Statement statement;
  ResultSet resultset;
  ResultSetMetaData metaData;
  String sql;
  String rowline;
  String server_ip;
  String server_uname;
  String server_port;
  String server_pass;
    
  public HiveJdbc(){
	  
	  try{
		  ConfigReader configreader = new ConfigReader();
		  server_ip=configreader.getProperty("server_ip");
		  server_pass=configreader.getProperty("server_pass");
		  server_uname=configreader.getProperty("server_uname");
		  server_port=configreader.getProperty("server_port");
	  }catch(Exception e){
		  System.err.println("Error reading configuration file in hivejdbc connection: " + e);
	  }
	  
	  try {
	      Class.forName(driverName);
	    } catch (ClassNotFoundException e) {
	      e.printStackTrace();
	      System.exit(1);
	    }
	  
	  try {
		String connectionString = String.format("jdbc:hive://%s:%s/",server_ip,server_port);
		connection = DriverManager.getConnection(connectionString, server_uname, server_pass);
	  } catch (SQLException e) {
		System.err.println("Error in setting up connection to host");
		e.printStackTrace();
	  }
	  try {
		statement = connection.createStatement();
	  } catch (SQLException e) {
		System.err.println("Error in setting up connection statement");
		e.printStackTrace();
	  }
	  
  }
  
  public void execute(String hiveQuery){
	  sql = hiveQuery;
		System.out.println("Running: " + sql);
		try{
			resultset = statement.executeQuery(hiveQuery);
		}catch(SQLException e){
			System.err.println("Error in executing hiveQuery " + hiveQuery);
			e.printStackTrace();
		} 
		
  }
  
  public void executeAndDisplay(String hiveQuery){
	sql = hiveQuery;
	System.out.println("Running: " + sql);
	try{
		
		statement.executeQuery(hiveQuery);
	
		int numberOfColumns = metaData.getColumnCount();
	    
	    while (resultset.next()) {
	    	for (int i =1; i<= numberOfColumns; i++){
	    		rowline = resultset.getString(i);
	    		System.out.println(rowline);
	    	
	    	//System.out.println(resultset.getString(1));
	    	}
	    }     
	}catch(SQLException e){
		System.err.println("Error in executing hiveQuery: " + e);
		e.printStackTrace();
	}
  }
  
  public void closeConnection(){
	try {
		if (!connection.isClosed())
			connection.close();
	} catch (SQLException e) {
		System.out.println("Error: cannot close connection");
		e.printStackTrace();
	}
  }
  
}
  
  