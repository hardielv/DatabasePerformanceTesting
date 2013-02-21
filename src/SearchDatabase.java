import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

public class SearchDatabase {
	final static String ppathLinux = "/home/m113216/scratch/";
	final static String fdirLinux = "/home/m113216/orient/datafiles/";
	
	final static String ppathWindows = "C:/scratch/";
	final static String fdirWindows = "C:/scratch/OrientDB/datafiles/";

	final static String [] headers = {"Date", "HeapSize", "DB_TYPE", "DB_Size", "Query", "Iterations", "Depth", "#V", "#E", "AvgRecs", "AvgMS", "AvgMin", "AvgSec"};
	
	final static int iterations = 1;
	final static int minDepth = 2, maxDepth = 2;
	
//	final static String DELIMITER = ",";
	final static String DELIMITER = "\t";
	
	// DATABASE TYPE SPECIFIC
	String DB_TYPE;
	DatabaseConnection_Interface db;
	
	String performanceFile;
	String performancePath;
	String fileDirectory;

	DatabaseType dbType;
	
	
	File performance;
	FileWriter fstream;
	BufferedWriter out;
	
	GlobalEnvironment env;
	
	public void openDatabase(){ db.open(); }
	public void closeDatabase() { db.close(); }
	public void collectRandomRIDs(int iterations) { db.collectRandomRIDs(iterations); }
	public String getDatabaseName() { return db.getDatabaseName(); }
	
	// Constructor
	public SearchDatabase(GlobalEnvironment e){
		String dbSize = databaseSizeToString(env.dbSize);
		env = e;
		
		if(env.osType == OperatingSystemType.LINUX){
			 performancePath = ppathLinux; 
			 fileDirectory = fdirLinux + "randomDB_" + env.dbSize + "/"; 
		}
		else{
			 performancePath = ppathWindows;
			 fileDirectory = fdirWindows + "randomDB_" + env.dbSize + "/";			
		}
		
		DB_TYPE = databaseTypeToString(dbType);
		performanceFile = DB_TYPE + "_results.txt";
				
		if(dbType == DatabaseType.ORIENTDB){
			db = new DatabaseConnection_OrientDB(env, dbSize);
		}
		else{
			db = new DatabaseConnection_MySQL(env, dbSize);
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
		} catch (IOException ex) {
			ex.printStackTrace();
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
	
	public void printToFile(String msg){
		try {
			out.write(msg);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public HashSet<Object> runQuery(QueryType query, int index, int depth) throws OutOfMemoryError{
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
	
	
	public boolean timePerformance(QueryType query, int iterations, int depth){
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
	

	public void print(String date, long memory, DatabaseSize size, QueryType query, int iterations, int depth){
		System.out.print("Iterations = " + iterations + ", depth = " + depth);
		System.out.println(", Database: " + env.DB_PATH);
		printToFile(date.toString() + DELIMITER + memory + DELIMITER + DB_TYPE + DELIMITER);
		printToFile(databaseSizeToString(size) + DELIMITER + stringQueryType(query) + DELIMITER + iterations + DELIMITER + depth);
	}
	
}
