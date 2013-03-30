package Analytics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Launch {

	private static String getMonth(String monyear){
		Map<String, String> months = new HashMap<String, String>();
		String month=monyear.substring(0, 3);
		String[] mons = {"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"};
		String[] val = {"01","02","03","04","05","06","07","08","09","10","11","12"};
		for (String m : mons){
			for (String v : val){
				months.put(m, v);
			}
		}
		return (months.get(month));
	}
	
	private static String getYear(String monyear){
		return monyear.substring(3, monyear.length());
	}
	
	public static void createTable( HiveJdbc hivejdbc, ConfigReader configreader){
		String tablename=configreader.getProperty("table_name");
		String col_list=configreader.getProperty("col_list");
		String partition=configreader.getProperty("partition");
		String field_delimiter=configreader.getProperty("field_delimiter");
		String line_terminator=configreader.getProperty("line_terminator");
		String escape_character=configreader.getProperty("escape_character");
		String stored_as=configreader.getProperty("stored_as");
		
		String hql = String.format("create table if not exists %s ( %s ) partitioned by ( %s ) row format delimited " +
				"fields terminated by '%s' escaped by '%s' lines terminated by '%s' " +
				"stored as %s", tablename,col_list, partition, field_delimiter, escape_character, line_terminator,stored_as);
		System.out.println(hql);
		try{
			hivejdbc.execute(hql);
			System.out.println("Query success!");
		}catch(Exception e){
			System.err.println("Could not execute hql");
		}
		
	}
	
	public static void dropTable( HiveJdbc hivejdbc, ConfigReader configreader){
		String tablename=configreader.getProperty("table_name");
		
		String hql = String.format("drop table %s", tablename);
		System.out.println(hql);
		try{
			hivejdbc.execute(hql);
			System.out.println("Query success!");
		}catch(Exception e){
			System.err.println("Could not execute hql");
		}
	}
	
	public static void loadFile(ConfigReader configreader, HDFSClient client){
		String source=configreader.getProperty("source");
		String[] files=source.split(",");
		String destination=configreader.getProperty("destination");
		try {
			for (int i=0; i< files.length; i++){
				System.out.printf("Loading file %s%n", files[i]);
				client.copyFromLocal(files[i], destination);
			}
			System.out.println("Done loading files.");
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	
	public static void copyFile(ConfigReader configreader, HDFSClient client){
		String source=configreader.getProperty("source");
		String destination=configreader.getProperty("destination");
		try {
			client.copyToLocal(source, destination);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	
	public static void overwriteTable(ConfigReader configreader, HiveJdbc hivejdbc){
		String from_table = configreader.getProperty("from_table");
		String to_table = configreader.getProperty("to_table");
		String overwrite_col_list = configreader.getProperty("overwrite_col_list");
		String overwrite_partition = configreader.getProperty("overwrite_partition");
		String operator = configreader.getProperty("operator");
		String join_clause = configreader.getProperty("join_clause");
		String where_clause = configreader.getProperty("where_clause");
		
		String circle_list = configreader.getProperty(String.format("%s_circles", operator));
		String dates = configreader.getProperty("dates");
		String monyear= configreader.getProperty("monyear");
		
		String month = getMonth(monyear);
		String year = getYear(monyear);
		String hql;
		
		System.out.printf("%s %s %n", month, year);
		
		
		String[] circle_list_array = circle_list.split(" ");
		String[] dates_array = dates.split(" ");
		
		for (String c : circle_list_array){
			for (String d : dates_array){
				String current_date= String.format("%s-%s-%s", year, month, d);
				System.out.printf("Circle: %s Day: %s%n", c, d);
				
				hql = String.format("from %s insert overwrite table %s partition (operator='%s', circle='%s', monyear='%s', calldate='%s') select %s"
						, from_table, to_table, operator, c, monyear, current_date, overwrite_col_list);
				System.out.println(hql);
			}
		}
		
		
	}
	
	public static void main (String[] args){
		
		HiveJdbc hivejdbc = new HiveJdbc();
		ConfigReader configreader = new ConfigReader();
		HDFSClient client = new HDFSClient();
		
	
	}
	
}
