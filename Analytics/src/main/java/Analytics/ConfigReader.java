package Analytics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class ConfigReader {

	Properties configFile;
	
	public ConfigReader(){
		
		try {
			configFile = new Properties();
			InputStream is = new FileInputStream("config"+File.separator+"table.properties");
			try {
				configFile.load(is);
			} catch (IOException e) {
				System.err.println("Error in loading the configfile");
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			System.out.println("Error : config file not found");
			e.printStackTrace();
		}
	}
	
	public String getProperty(String key){
		String value = this.configFile.getProperty(key);
		return value;
	}
	
}
