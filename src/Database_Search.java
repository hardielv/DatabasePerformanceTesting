import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashSet;
import java.util.Iterator;

public class Database_Search {
	final static int iterations = 1;
	final static int minDepth = 2, maxDepth = 2;
	
	// DATABASE TYPE SPECIFIC
	String DB_VENDOR;
	Database_Connection_Interface db;
	
	String resultsFile;
	String resultsDir;

	Database_Vendor dbVendor;	
	PerformanceResults testResults;
	
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
		testResults = new PerformanceResults();
		
		DB_VENDOR = dbVendor.toString();
		resultsFile = DB_VENDOR + "_results.txt";
				
		if(dbVendor == Database_Vendor.ORIENTDB){
			db = new Database_Connection_OrientDB(env);
		}
		else{
			db = new Database_Connection_MySQL(env);
		}
		
		openPerformanceFile();
	}

	private void openPerformanceFile(){
		boolean addHeaders  = true;
		performance = new File(resultsDir + resultsFile);
		if(performance.exists()) addHeaders = false;
		try{
			fstream = new FileWriter(performance, true);
			out = new BufferedWriter(fstream);
			if(addHeaders){
				printToFile(testResults.getHeaders() + "\n");
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

	public void testQuery(Database_QueryType query, int iterations, int depth) throws OutOfMemoryError{
		HashSet<Object> graph = null; 

		for(int i=0; i < iterations; i++){
			System.out.println("Testing query: " + query.toString());
			
			graph = runQuery(Database_QueryType.TRAVERSE, i, depth);
			System.out.println("Traverse of RID(" + db.getRandomRID(i) + ") to depth(" + depth + ")");
			printResults(graph);

			if(query != Database_QueryType.TRAVERSE){
				System.out.println("Traverse of RID(" + db.getRandomRID(i) + ") to depth(" + depth + ")");
				graph = runQuery(Database_QueryType.TRAVERSE, i + 1, depth);			
				printResults(graph);

				System.out.println("Results of query: " + query.toString());
				graph = runQuery(query, i, depth);
				printResults(graph);
			}
		}
	}

	public void printResults(HashSet<Object> graph){
		Iterator<Object> iterator = graph.iterator();
		while(iterator.hasNext()){
			System.out.print(iterator.next() + ", ");
		}
		
		System.out.println("\n");
	}
	
	
	public HashSet<Object> runQuery(Database_QueryType query, int index, int depth) throws OutOfMemoryError{
		int graphID_1 = 1, graphID_2 = 2;
		int initialLevel = 0;
		
		db.getGraph(graphID_1).clear();
		db.traverseJava(db.getRandomRID(index), initialLevel, depth, graphID_1);

		if(query != Database_QueryType.TRAVERSE){
			db.getGraph(graphID_2).clear();  
			db.traverseJava(db.getRandomRID(index + 1), initialLevel, depth, graphID_2);
			if(query == Database_QueryType.UNION){
				db.getGraph(1).addAll(db.getGraph(graphID_2));
			}
			else if(query == Database_QueryType.DIFFERENCE){
				db.getGraph(1).removeAll(db.getGraph(graphID_2));
			}
			else if(query == Database_QueryType.INTERSECTION){
				db.getGraph(1).retainAll(db.getGraph(graphID_2));				
			}
			else if(query == Database_QueryType.SYMMETRIC_DIFFERENCE){
				HashSet<Object> symmetricDiff = new HashSet<Object>(db.getGraph(graphID_1));
				symmetricDiff.addAll(db.getGraph(2));
				HashSet<Object> tmp = new HashSet<Object>(db.getGraph(graphID_1));
				tmp.retainAll(db.getGraph(graphID_2));
				symmetricDiff.removeAll(tmp);
				return symmetricDiff;
			}
		}
		
		return db.getGraph(graphID_1);
	}
	
	
	public boolean timePerformance(Database_QueryType query, int iterations, int depth){
		HashSet<Object> results = null;
		
		ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
//		if(threadBean.isCurrentThreadCpuTimeSupported()){
//			System.out.println("measuring cpu time");
//		}
			
		long [] times = new long[iterations];
		long [] cpuTimes = new long[iterations];
		int numRecords = 0;
		
		long startTime = 0, endTime = 0;
		int minutes, seconds;
		long cpuStart, cpuEnd;
		int cpuMin, cpuSec;
		
		System.out.println("----------------------------------------");
		System.out.println("Timing " + query.toString() + " of graph with " + env.getVertexCount() + " vertices");
		testResults.saveField(PerformanceResultFields.VERTEX_COUNT, "" + env.getVertexCount());
		testResults.saveField(PerformanceResultFields.EDGE_COUNT, "" + env.getEdgeCount());
		long totalTimes = 0;
		long cpuTotalTimes = 0;
		boolean outOfMemory = false;
		for(int i=0; i < iterations; i++){
			System.out.print((i + 1) + " ... ");
			try{
				// Time query
				cpuStart = threadBean.getCurrentThreadCpuTime();
				startTime = System.currentTimeMillis();
				results = runQuery(query, i, depth);
				endTime = System.currentTimeMillis();
				cpuEnd = threadBean.getCurrentThreadCpuTime();
	
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
			
			cpuTimes[i] = (cpuEnd - cpuStart)/1000000;
			cpuMin = (int) (cpuTimes[i] / (1000 * 60));
			cpuSec = (int) ((cpuTimes[i] / 1000) % 60);
			cpuTotalTimes += cpuTimes[i];
		}
		System.out.println();
		
		if(!outOfMemory){
			long avgTime = totalTimes / iterations;
			long cpuAvg = cpuTotalTimes / iterations;
			minutes = (int) (avgTime / (1000 * 60));
			seconds = (int) ((avgTime / 1000) % 60);
			cpuMin = (int) (cpuAvg / (1000 * 60));
			cpuSec = (int) ((cpuAvg / 1000) % 60);

			
			System.out.println("Average #records: " + (numRecords / iterations) + " of " + env.getVertexCount());
			System.out.println(String.format("Average Time: %d ms or (%d min, %d sec)", avgTime, minutes, seconds)); 
			System.out.println(String.format("Average CPU Time: %d ms or (%d min, %d sec)\n", cpuAvg, cpuMin, cpuSec)); 

			testResults.saveField(PerformanceResultFields.AVG_RECORDS, "" + (numRecords / iterations));
			testResults.saveField(PerformanceResultFields.AVG_TIME_MS, "" + avgTime);
			testResults.saveField(PerformanceResultFields.AVG_TIME_MIN, "" + minutes);
			testResults.saveField(PerformanceResultFields.AVG_TIME_SEC,  "" + seconds);
			
			testResults.saveField(PerformanceResultFields.AVG_CPU_TIME_MS, "" + cpuAvg);
			testResults.saveField(PerformanceResultFields.AVG_CPU_TIME_MIN, "" + cpuMin);
			testResults.saveField(PerformanceResultFields.AVG_CPU_TIME_SEC,  "" + cpuSec);
			printToFile(testResults.toString() + "\n");
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
		testResults.saveField(PerformanceResultFields.DATE, date.toString());
		testResults.saveField(PerformanceResultFields.HEAPSIZE, "" + memory);
		testResults.saveField(PerformanceResultFields.DB_VENDOR, "" + DB_VENDOR);
		testResults.saveField(PerformanceResultFields.DB_NAME, "" + env.getDatabaseName());
		testResults.saveField(PerformanceResultFields.QUERY, "" + query.toString());
		testResults.saveField(PerformanceResultFields.ITERATIONS, "" + iterations);
		testResults.saveField(PerformanceResultFields.DEPTH, "" + depth);		
	}
	
}
