import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;

public class SearchEngine {
	final static String fileDirectory = "/home/m113216/orient/datafiles/importDB_huge";
//	final static DatabaseType [] dbTypes = {DatabaseType.MYSQL};
	final static DatabaseType [] dbTypes = {DatabaseType.ORIENTDB};
	
	final static DatabaseSize [] size = {DatabaseSize.SMALL, DatabaseSize.MEDIUM, DatabaseSize.LARGE, DatabaseSize.HUGE}; 
//	final static DatabaseSize [] size = {DatabaseSize.SMALL}; 
	
	final static QueryType [] queryList = {QueryType.TRAVERSE, QueryType.INTERSECTION, QueryType.UNION, QueryType.DIFFERENCE};
//	final static QueryType [] queryList = {QueryType.TRAVERSE};
	
	
	final static OperatingSystemType osType = OperatingSystemType.WINDOWS;
//	final static OperatingSystemType osType = OperatingSystemType.LINUX;
	
	
	final static int iterations = 1;
	final static int minDepth = 2, maxDepth = 2;
	
	final static String [] headers = {"Date", "HeapSize", "DB_TYPE", "DB_Size", "Query", "Iterations", "Depth", "#V", "#E", "AvgRecs", "AvgMS", "AvgMin", "AvgSec"};

	
//	final static String DELIMITER = ",";
	final static String DELIMITER = "\t";
	
	// DATABASE TYPE SPECIFIC
	String DB_TYPE;
	DatabaseConnection_Interface db;
	
	String performanceFile;
	String performancePath;


	File performance;
	FileWriter fstream;
	BufferedWriter out;
			
	public static void main(String[] args) {
		SearchDatabase searchDB; 
		long memory = Runtime.getRuntime().totalMemory();
		Date date = new Date();
		
		GlobalEnvironment env;
		
		try{
			for(int i=0; i < size.length; i++){
				System.out.println("-----------------------------");
				System.out.println("Connecting to " + size[i]);
				env = new GlobalEnvironment(osType, dbTypes[0], size[i]);
				boolean completed = true;
				for(int k=0; k < queryList.length; k++){
					for(int depth=minDepth; completed && depth <= maxDepth; depth++){
						searchDB = new SearchDatabase(env);
						
						searchDB.print(date.toString(), memory, size[i], queryList[k], iterations, depth);
						
						searchDB.openDatabase();
						searchDB.collectRandomRIDs(iterations);
						completed = searchDB.timePerformance(queryList[k], iterations, depth);
	
						searchDB.closeDatabase();
						searchDB.closeFiles();
					}
					System.out.println();
				}
			}
		} catch(Exception e){
			System.out.println("Some sort of error occurred.");
			e.printStackTrace();
		}
		System.out.println("Done");
	}	
}
