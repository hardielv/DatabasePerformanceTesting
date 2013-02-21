import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;


public class dbConnection_OrientDB implements DatabaseConnection_Interface {
	String DB_TYPE = "orientdb";
	String DB_PATH;
	String DB_NAME;
	OGraphDatabase db;
	static final String USER = "admin";
	static final String PASS = "admin";
	
	DatabaseEnvironment env;
	String [] randomRID_list;

	private HashSet<Object> GRAPH1, GRAPH2;
	
	
	public dbConnection_OrientDB(DatabaseEnvironment e, String size){
		env = e;
		DB_NAME = env.DB_NAME_PREFIX + size;
		DB_PATH = env.DB_PATH;
		GRAPH1 = new HashSet<Object>();
		GRAPH2 = new HashSet<Object>();
		open();
	}

	@Override
	public void open() {
		File dir = new File(DB_PATH);
		if(!dir.exists()){
			System.out.println("Database does not exist");
			System.exit(1);
		}
		
		db = new OGraphDatabase("local:" + DB_PATH);
		db.open("admin",  "admin");
	}

	@Override
	public void close() {
		db.close();
	}

	@Override
	public String getDatabaseName(){
		return DB_NAME;
	}
	
	@Override
	public Object getRandomRID(int i){ 
		return randomRID_list[i];
	}
	
	@Override
	public void setRandomRID(int i, Object value){
		randomRID_list[i] = (String) value;
	}
	
	@Override
	public void collectRandomRIDs(int iterations) {
		// Add #iterations plus one for the set operations to have two subgraphs per iteration
		int numRIDs = iterations + 1;
		randomRID_list = new String[numRIDs];
		String randomClusterName;
		int clusterID, randomPosition;
		
		// Collect #iterations of random RID's
		for(int i=0; i < numRIDs; i++){
			randomClusterName = env.VERTEX_PREFIX + (int) (Math.random() * env.NUM_VERTEX_TYPE);
			clusterID = db.getClusterIdByName(randomClusterName); 
			OClusterPosition [] range = db.getStorage().getClusterDataRange(clusterID);
			
			randomPosition = (int) (Math.random() * range[1].intValue()) + range[0].intValue();
			randomRID_list[i] = "#" + clusterID + ":" + randomPosition;
		}
	}

	@Override
	public HashSet<Object> traverseJava(Object root, int level, int depth, int graphID) {
		String sql;
		ArrayList<ODocument> results = null;
		String id;
		sql = createSQL_Traverse((String) root, depth);
		results = db.query(new OSQLSynchQuery<OIdentifiable>(sql));

		for (int i = 0; i < results.size(); i++) {
			// System.out.println(results.get(i));
			id = ((ODocument) results.get(i).field("rid1")).getIdentity()
					.toString();
			// System.out.println(doc);
			// String id = doc.getIdentity().toString();
			// System.out.println(id);
			// System.out.println();
			if (graphID == 1) {
				GRAPH1.add(id);
			} else {
				GRAPH2.add(id);
			}
		}
		
		if(graphID ==1) return GRAPH1;
		return GRAPH2;
	}


	@Override
	public HashSet<Object> getGraph(int i) {
		if(i == 1) return GRAPH1;
		return GRAPH2;
	}

	
	@Override
	public void printRecords(List<Object> recList){	
		System.out.println("Printing records: ");
				
		for(Object record : recList) {
			OIdentifiable r = (OIdentifiable) record;
			System.out.println(r.toString());
		}
		System.out.println("Done\n\n");
	}
	
	
	
	// ------------------------------------------------------------------------------------------
	private String createSQL_Traverse(String id, int depth){
		String sql;
		
		sql =  "SELECT @RID AS rid1, label, $depth FROM ";
		sql +=     "(TRAVERSE V.in, V.out, E.out FROM " + id + " WHILE $depth <= " + depth + ") ";
		sql += " WHERE label LIKE 'Vertex%'";
		return sql;
	}
	
	private void traverse_Iterator(int iterations, int depth){
		int numRecords = 0, records = 0;
		long [] times = new long[iterations];		
		long totalTimes = 0;
		long startTime, endTime;
		int minutes, seconds;
//		String rid = "#11:272915";
//		depth = 8;
		OIdentifiable id;
		System.out.println("Timing Iterator of " + iterations + " traversals of graph with " + env.TOTAL_VERTICES + " vertices");
		System.out.println("With depth = " + depth);
		
		for(int i=0; i < iterations; i++){
			records = 0;
//			System.out.println("Working with: " + rid);
			System.out.println("Working with: " + randomRID_list[i]);
			startTime = System.currentTimeMillis();
			Iterator<? extends OIdentifiable> g = new OTraverse().target(new ORecordId(randomRID_list[i])).fields("V.out", "V.in", "E.out").predicate(new OSQLPredicate("$depth <= " + depth));
//			OTraverse graph = new OTraverse().target(new ORecordId(rid)).fields("V.out", "V.in", "E.out").predicate(new OSQLPredicate("$depth <= " + depth));
//			OTraverse graph = new OTraverse().target(new ORecordId(rid)).fields("out", "in").predicate(new OSQLPredicate("$depth <= " + depth));
//			Iterator<? extends OIdentifiable> g = graph;
//			System.out.println("First ID: " + g.next());
//			graph.execute();

			while(g.hasNext()){
				id=g.next();
			System.out.println("ID: " + id);
			}
			
//			for(int j=0; j < 10 && g.hasNext(); j++){
//				id=g.next();
////				System.out.println("ID: " + id);
//			}
			//			for(OIdentifiable id : graph){
//				records++;
//			}
//			for (OIdentifiable id : new OTraverse()
//									.target(new ORecordId(randomRID_list[i]))
//									.fields("out", "in")
//									.predicate( new OSQLPredicate("$depth <= " + depth))) {
//	
//				System.out.println( id);
//				records++;
//			}				
			endTime = System.currentTimeMillis();
			
			numRecords += records;			
			times[i] = endTime - startTime;
			minutes = (int) (times[i] / (1000 * 60));
			seconds = (int) ((times[i] / 1000) % 60);
			totalTimes += times[i];
		}
		
		long avgTime = totalTimes / iterations;
		minutes = (int) (avgTime / (1000 * 60));
		seconds = (int) ((avgTime / 1000) % 60);
		
		System.out.println(iterations + " iterations, " + depth + " depth");
		System.out.println("Average #records: " + (numRecords / iterations) + " of " + env.TOTAL_VERTICES);
		System.out.println(String.format("Average Time: %d ms or (%d min, %d sec)", avgTime, minutes, seconds)); 
		System.out.println();					
	}
}
