package com.one97.dwh.hbase;
/*
 * Hbase CRUD operations tools.
 *
 * */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;


public class Hbase {
	
	/* Function printUsage()
	 * Prints out usage details of all the available functions.
	 */
	public static void printUsage(){
		System.out.println("Usage: hadoop jar <jar_file> Hbase getvalue <tablename> <rowkey> <coloumn_family> <coloumn_qualifer>");
		System.out.println("Usage: hadoop jar <jar_file> Hbase putvalue <tablename> <rowkey> <coloumn_family> <coloumn_qualifer> <value>");
		System.out.println("Usage: hadoop jar <jar_file> Hbase loadfile <tablename> <path> <delimiter>");
		System.out.println("Usage: hadoop jar <jar_file> Hbase deleterow <tablename> <rowkey> <coloumn_family> <coloumn_qualifer>");
		System.out.println("Usage: hadoop jar <jar_file> Hbase scan <tablename> <rowkey> <coloumn_family>");
	}

	public static void scan(Configuration conf, HTable table, String rowkey, String cfamily) throws IOException{
		
		Scan scan = new Scan(); 
		
		scan.addFamily(Bytes.toBytes(cfamily)); // Add one column family only, this will suppress the retrieval of "colfam2".
		
		//scan.addColumn(Bytes.toBytes(cfamily), Bytes.toBytes(qualifier));
		
		//do a filter based on rowkey
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(rowkey)));
		scan.setFilter(filter);
		
		//do a filer based on cfamily
		//Filter filter2 = new FamilyFilter(CompareFilter.CompareOp.LESS,new BinaryComparator(Bytes.toBytes("cfamily")));
		//do a filter based on qualifier
		//Filter filter = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(qualifier)));
		//do a filter based on value
		//Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(".4"));
		
		
		ResultScanner scanner = table.getScanner(scan);
		
		System.out.println("Rowkey: " + rowkey );
		
		for (Result r : scanner){
			String result_string = r.toString();
			
			String[] values = result_string.split(",");
			
			for ( String v : values ){
					String[] cf = v.split("/");
					String[] name = cf[1].split(":");			
					
					byte[] val = r.getValue(Bytes.toBytes(cfamily),Bytes.toBytes(name[1]));
					System.out.println("Qualifier: " + name[1] + " Value: " + Bytes.toString(val));
				}
			}
		
		scanner.close();
	}
	
	/* Function deleterow
	 * Deletes a specific row from the HBase table. 
	 * To do: delete #3 and #5
	 */
	public static void deleterow(Configuration conf, HTable table ,String rowkey, String cfamily, String qualifier) throws IOException{
		
		getvalue(conf, table, rowkey, cfamily, qualifier); // Display value to be deleted.
		Delete delete = new Delete(Bytes.toBytes(rowkey)); //Create delete with specific row.
		
		//delete.setTimestamp(1); //Set timestamp for row deletes.
		
		delete.deleteColumns(Bytes.toBytes(cfamily), Bytes.toBytes(qualifier)); //Delete all versions in one column.
		table.delete(delete); 
		
		System.out.println("Deleted.");
	}
		
	/* Function loadfile()
	 * Used to load a single file < with a given format, one CF >
	 */
	public static void loadfile(Configuration conf, HTable table, String fpath, String delimiter) throws IOException{
		
		try {
			FileInputStream fstream = new FileInputStream(fpath);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			
			while ((strLine = br.readLine()) != null)   {
				String word[] = strLine.split(delimiter);
				
				if (word.length != 4){
					System.err.println("Invalid file format: ");
					System.out.println("Required format : KEY <delimiter> CF <delimiter> QF <delimiter> VAL");
				}
				
				for(int i=0; i<word.length; i++){
					String key = word[0];
					String cf = word[1];
					String qf = word[2];
					String vl = word[3];
					
					Put myput = new Put(Bytes.toBytes(key));
					myput.add(Bytes.toBytes(cf), Bytes.toBytes(qf),Bytes.toBytes(vl));
					table.put(myput);
					
					System.out.println("CF: " + cf + " QF: " + qf + " VAL: " + vl);
					//System.out.println (strLine);
				}
			}
			
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	

	/* Function putvalue() 
	 * Puts a single value into the HBase table based on rowkey, cfamily and qulifier.
	 */
	public static void putvalue(Configuration conf, HTable table, String rowkey, String cfamily, String qualifier, String value) throws IOException {
		
		Put put = new Put(Bytes.toBytes(rowkey)); //Create put with specific row.
		
		put.add(Bytes.toBytes(cfamily), Bytes.toBytes(qualifier), Bytes.toBytes(value)); //Add cf, qf and values
		
		table.put(put); 
		
		System.out.println("Value: " + value + " added.");
	}

	
	/* Function getvalue()
	 * Retrieves a row from the HBase table based on rowkey, cfamily and qulifier.
	 */
	public static void getvalue(Configuration conf, HTable table, String rowkey, String cfamily, String qualifier) throws IOException{
		
		Get get = new Get(Bytes.toBytes(rowkey));//Create get with specific row.
		
		get.addColumn(Bytes.toBytes(cfamily), Bytes.toBytes(qualifier));//Add a column to the get.
		
		Result result = table.get(get); 
		byte[] val = result.getValue(Bytes.toBytes(cfamily),Bytes.toBytes(qualifier));
		
		System.out.println("Value: " + Bytes.toString(val));
	}
	
	public static void bulkload(Configuration conf, HTable table, String fpath) throws IOException{
		
		LoadIncrementalHFiles loadinchfiles = new LoadIncrementalHFiles(conf);
		loadinchfiles.doBulkLoad(new Path(fpath), table);
	}
	
	/* Main function
	 */
	public static void main (String args[]) throws IOException{
		
		//Primary argument checking
		if (args.length < 2){
			System.err.println("Insufficient argument list: ");
			printUsage();
		    System.exit(1);
		}
		
		//Create the required configuration.
		Configuration conf = HBaseConfiguration.create();
		
		//Prints out the given arguments 
		for (int i =0;i<args.length; i++){
			System.out.println("Argument" + i + ":" + args[i]);
		}
		
		//Instantiate a new client.
		HTable table = new HTable(conf,args[2]);
		
		
		
		String get_function = new String(args[1]);
		
		try{
		
		if (( get_function.equalsIgnoreCase("getvalue") && (args.length==6))){
			String rowkey = new String(args[3]);
			String cfamily = new String(args[4]);
			String qualifier = new String(args[5]);
			
			getvalue(conf, table, rowkey, cfamily, qualifier);
			
		}else if ((get_function.equalsIgnoreCase("putvalue") && (args.length==7))){
			String rowkey = new String(args[3]);
			String cfamily = new String(args[4]);
			String qualifier = new String(args[5]);
			String value = new String(args[6]);
			
			putvalue(conf, table, rowkey, cfamily, qualifier,value);
			
		}else if ((get_function.equalsIgnoreCase("loadfile") && (args.length==5))){
			String path = new String(args[3]);
			String delimiter = new String(args[4]);
			System.out.println("Path :" + path);
			System.out.println("Delimiter :" + delimiter);
			
			loadfile(conf, table, path, delimiter);
			
		}else if ((get_function.equalsIgnoreCase("deleterow") && (args.length==6))){
			String rowkey = new String(args[3]);
			String cfamily = new String(args[4]);
			String qualifier = new String(args[5]);
			
			deleterow(conf, table, rowkey, cfamily, qualifier);
			
		}else if ((get_function.equalsIgnoreCase("scan") && (args.length==5))) {
			String rowkey = new String(args[3]);
			String cfamily = new String(args[4]);
			
			scan(conf,table,rowkey,cfamily);
		}else if ((get_function.equalsIgnoreCase("bulkload") && (args.length==4))){
			String fpath = new String(args[3]);
			bulkload(conf, table, fpath);
		}
		else{
			System.err.println("Unknown function");
			//printUsage
			printUsage();
			System.exit(1);
		}

		}catch(Exception e){
			System.err.println("Error " + e);
			printUsage();
			table.close();
		}
		
		table.close(); // close the HBase table instance.
	}

}
