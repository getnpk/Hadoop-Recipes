
import java.io.File;
import java.util.Date;


public class DeleteTemp{

	public static void main (String[] args){
		delete(new File("/tmp/hadoop"));
	}

	public static void delete(File f){
	Date d = new Date();

	try{
		if (f.isDirectory()){
			String[] files = f.list();
			for(int i=0 ; i< files.length; i++){
				File fthis = new File(files[i]);
				if (fthis.isDirectory())
					delete(fthis);
				long timediff = d.getTime() - fthis.lastModified();
		                long hours = (timediff/360000);
        		        if (hours > 2){
                		        System.out.println(fthis);
               			}
			}
		}
		
	}catch(Exception e){
		e.printStackTrace();
	}

	}	
}

