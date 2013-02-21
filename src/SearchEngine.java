import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;

public class SearchEngine {
	public enum QueryType {TRAVERSE, UNION, INTERSECTION, DIFFERENCE, SYMMETRIC_DIFFERENCE};
	public enum DatabaseType {ORIENTDB, MYSQL};
	public enum OperatingSystemType {WINDOWS, LINUX};
	public enum DatabaseSize {SMALL, MEDIUM, LARGE, HUGE};
	
//	final static DatabaseType [] dbTypes = {DatabaseType.MYSQL};
	final static DatabaseType [] dbTypes = {DatabaseType.ORIENTDB};
	
	final static DatabaseSize [] size = {DatabaseSize.SMALL, DatabaseSize.MEDIUM, DatabaseSize.LARGE, DatabaseSize.HUGE}; 
	final static QueryType [] queryList = {QueryType.TRAVERSE, QueryType.INTERSECTION, QueryType.UNION, QueryType.DIFFERENCE};
//	final static DatabaseSize [] size = {DatabaseSize.SMALL}; 
//	final static QueryType [] queryList = {QueryType.TRAVERSE};
	
	
	final static OperatingSystemType osType = OperatingSystemType.WINDOWS;
//	final static OperatingSystemType osType = OperatingSystemType.LINUX;
	
	final static String ppathLinux = "/home/m113216/scratch/";
	final static String fdirLinux = "/home/m113216/orient/datafiles/";
	
	final static String ppathWindows = "C:/scratch/";
	final static String fdirWindows = "C:/scratch/OrientDB/datafiles/";
	
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

	DatabaseEnvironment env;
	String fileDirectory;

	File performance;
	FileWriter fstream;
	BufferedWriter out;
			
	public static void main(String[] args) {
		SearchEngine searchDB; 
		long memory = Runtime.getRuntime().totalMemory();
		Date date = new Date();
		
		try{
			for(int i=0; i < size.length; i++){
				System.out.println("-----------------------------");
				System.out.println("Connecting to " + size[i]);
				boolean completed = true;
				for(int k=0; k < queryList.length; k++){
					for(int depth=minDepth; completed && depth <= maxDepth; depth++){
						searchDB = new SearchEngine(osType, dbTypes[0], size[i]);
						
						System.out.print("Iterations = " + iterations + ", depth = " + depth);
						System.out.println(", Database: " + searchDB.env.DB_PATH);
						searchDB.printToFile(date.toString() + DELIMITER + memory + DELIMITER + searchDB.DB_TYPE + DELIMITER);
						searchDB.printToFile(searchDB.databaseSizeToString(size[i]) + DELIMITER + searchDB.stringQueryType(queryList[k]) + DELIMITER + iterations + DELIMITER + depth);
						
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

	public void openDatabase(){ db.open(); }
	public void closeDatabase() { db.close(); }
	public void collectRandomRIDs(int iterations) { db.collectRandomRIDs(iterations); }
	public String getDatabaseName() { return db.getDatabaseName(); }
	
	// Constructor
	public SearchEngine(OperatingSystemType os, DatabaseType dbType, DatabaseSize size){
		String dbSize = databaseSizeToString(size);
		
		if(os == OperatingSystemType.LINUX){
			 performancePath = ppathLinux; 
			 fileDirectory = fdirLinux + "randomDB_" + size + "/"; 
		}
		else{
			 performancePath = ppathWindows;
			 fileDirectory = fdirWindows + "randomDB_" + size + "/";			
		}
		
		DB_TYPE = databaseTypeToString(dbType);
		performanceFile = DB_TYPE + "_results.txt";
		
		env = new DatabaseEnvironment(fileDirectory);		
		
		if(dbTypes[0] == DatabaseType.ORIENTDB){
			db = new dbConnection_OrientDB(env, dbSize);
		}
		else{
			db = new dbConnection_MySQL(env, dbSize);
		}
		
		
		boolean addHeaders  = true;
		performance = new File(performancePath + performanceFile);
		if(performance.exists()) addHeaders = false;
		try{
			fstream = new FileWriter(performance, true);
			out = new BufferedWriter(fstream);
			if(addHeaders){
				for(int i = 0; i < headers.length; i++){
					printToFile(headers[i]);
					if(i < (headers.length - 1)){
						printToFile(DELIMITER);
					}
				}
				printToFile("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String databaseSizeToString(DatabaseSize size){
		String dbSize = "";
		
		if(size == DatabaseSize.SMALL) dbSize = "small";
		else if(size == DatabaseSize.MEDIUM) dbSize = "medium";
		else if(size == DatabaseSize.LARGE) dbSize = "large";
		else if(size == DatabaseSize.HUGE) dbSize = "huge";
		
		return dbSize;
	}
	
	public String databaseTypeToString(DatabaseType type){
		String name = "";
		if(type == DatabaseType.MYSQL) 	name = "mySQL";
		else 							name = "orientDB";
		
		return name;
	}
	
	
	public void closeFiles(){
		try {
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void printToFile(String msg){
		try {
			out.write(msg);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private HashSet<Object> runQuery(QueryType query, int index, int depth) throws OutOfMemoryError{
		db.getGraph(1).clear(); 
		
		db.traverseJava(db.getRandomRID(index), 0, depth, 1);

		if(query != QueryType.TRAVERSE){
			db.getGraph(2).clear();  
			db.traverseJava(db.getRandomRID(index + 1), 0, depth, 2);
			if(query == QueryType.UNION){
				db.getGraph(1).addAll(db.getGraph(2));
			}
			else if(query == QueryType.DIFFERENCE){
				db.getGraph(1).removeAll(db.getGraph(2));
			}
			else if(query == QueryType.INTERSECTION){
				db.getGraph(1).retainAll(db.getGraph(2));				
			}
			else if(query == QueryType.SYMMETRIC_DIFFERENCE){
				HashSet<Object> symmetricDiff = new HashSet<Object>(db.getGraph(1));
				symmetricDiff.addAll(db.getGraph(2));
				HashSet<Object> tmp = new HashSet<Object>(db.getGraph(1));
				tmp.retainAll(db.getGraph(2));
				symmetricDiff.removeAll(tmp);
				return symmetricDiff;
			}
		}
		
		return db.getGraph(1);
	}
	
	
	private boolean timePerformance(QueryType query, int iterations, int depth){
		HashSet<Object> results = null;
		
		long [] times = new long[iterations];
		int numRecords = 0;
		
		long startTime = 0, endTime = 0;
		int minutes, seconds;
		
		System.out.println("Timing " + stringQueryType(query) + " of graph with " + env.TOTAL_VERTICES + " vertices");
		printToFile(DELIMITER + env.TOTAL_VERTICES + DELIMITER + env.TOTAL_EDGES);
		long totalTimes = 0;
		boolean outOfMemory = false;
		for(int i=0; i < iterations; i++){
			System.out.print((i + 1) + " ... ");
			try{
				// Time query
				startTime = System.currentTimeMillis();
				results = runQuery(query, i, depth);
				endTime = System.currentTimeMillis();
	
				if(results != null)	{
					numRecords += results.size();
				}
				
				
			} catch (OutOfMemoryError oome){
				System.out.println("Out of memory");
				outOfMemory = true;
				break;
			}
			times[i] = endTime - startTime;
			minutes = (int) (times[i] / (1000 * 60));
			seconds = (int) ((times[i] / 1000) % 60);
			totalTimes += times[i];
		}
		System.out.println();
		
		if(!outOfMemory){
			long avgTime = totalTimes / iterations;
			minutes = (int) (avgTime / (1000 * 60));
			seconds = (int) ((avgTime / 1000) % 60);
			
			System.out.println("Average #records: " + (numRecords / iterations) + " of " + env.TOTAL_VERTICES);
			System.out.println(String.format("Average Time: %d ms or (%d min, %d sec)", avgTime, minutes, seconds)); 

			printToFile(DELIMITER + (numRecords / iterations));
			printToFile(DELIMITER + avgTime + DELIMITER + minutes + DELIMITER + seconds + "\n"); 
		}
		try {
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return !outOfMemory;
	}

	
	private String stringQueryType(QueryType t){
		String type = "";
		
		if(t == QueryType.TRAVERSE) type = "TRAVERSE";
		else if(t == QueryType.DIFFERENCE) type = "DIFFERENCE";
		else if(t == QueryType.INTERSECTION) type = "INTERSECTION";
		else if(t == QueryType.UNION) type = "UNION";
		else if(t == QueryType.SYMMETRIC_DIFFERENCE) type = "SYMMETRIC_DIFFERENCE";

		return type;
	}
	
}
