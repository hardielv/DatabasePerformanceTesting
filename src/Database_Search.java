import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

public class Database_Search {
	final static String [] headers = {"Date", "HeapSize", "DB_VENDOR", "DB_Name", "Query", "Iterations", "Depth", "#V", "#E", "AvgRecs", "AvgMS", "AvgMin", "AvgSec"};
	
	final static int iterations = 1;
	final static int minDepth = 2, maxDepth = 2;
	
//	final static String DELIMITER = ",";
	final static String DELIMITER = "\t";
	
	// DATABASE TYPE SPECIFIC
	String DB_VENDOR;
	Database_Connection_Interface db;
	
	String resultsFile;
	String resultsDir;

	Database_Vendor dbVendor;	
	
	File performance;
	FileWriter fstream;
	BufferedWriter out;
	
	Data_Common env;
	
	public void openDatabase(){ db.open(); }
	public void closeDatabase() { db.close(); }
	public void collectRandomRIDs(int iterations) { db.collectRandomRIDs(iterations); }
	public String getDatabaseName() { return db.getDatabaseName(); }
	
	// Constructor
	public Database_Search(Data_Common e, Database_Vendor vendor, String rDir){
		env = e;
		dbVendor = vendor;
		resultsDir = rDir;
		
		DB_VENDOR = dbVendor.toString();
		resultsFile = DB_VENDOR + "_results.txt";
				
		if(dbVendor == Database_Vendor.ORIENTDB){
			db = new Database_Connection_OrientDB(env);
		}
		else{
			db = new Database_Connection_MySQL(env);
		}
		
		
		boolean addHeaders  = true;
		performance = new File(resultsDir + resultsFile);
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

	public HashSet<Object> runQuery(Database_QueryType query, int index, int depth) throws OutOfMemoryError{
		db.getGraph(1).clear(); 
		
		db.traverseJava(db.getRandomRID(index), 0, depth, 1);

		if(query != Database_QueryType.TRAVERSE){
			db.getGraph(2).clear();  
			db.traverseJava(db.getRandomRID(index + 1), 0, depth, 2);
			if(query == Database_QueryType.UNION){
				db.getGraph(1).addAll(db.getGraph(2));
			}
			else if(query == Database_QueryType.DIFFERENCE){
				db.getGraph(1).removeAll(db.getGraph(2));
			}
			else if(query == Database_QueryType.INTERSECTION){
				db.getGraph(1).retainAll(db.getGraph(2));				
			}
			else if(query == Database_QueryType.SYMMETRIC_DIFFERENCE){
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
	
	
	public boolean timePerformance(Database_QueryType query, int iterations, int depth){
		HashSet<Object> results = null;
		
		long [] times = new long[iterations];
		int numRecords = 0;
		
		long startTime = 0, endTime = 0;
		int minutes, seconds;
		
		System.out.println("Timing " + query.toString() + " of graph with " + env.getVertexCount() + " vertices");
		printToFile(DELIMITER + env.getVertexCount() + DELIMITER + env.getEdgeCount());
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
			
			System.out.println("Average #records: " + (numRecords / iterations) + " of " + env.getVertexCount());
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
	
	public void print(String date, long memory, Database_QueryType query, int iterations, int depth){
		System.out.print("Iterations = " + iterations + ", depth = " + depth);
		System.out.println(", Database: " + env.getDatabaseName());
		printToFile(date.toString() + DELIMITER + memory + DELIMITER + DB_VENDOR + DELIMITER);
		printToFile(env.getDatabaseName() + DELIMITER + query.toString() + DELIMITER + iterations + DELIMITER + depth);
	}
	
}
