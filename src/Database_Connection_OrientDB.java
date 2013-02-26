import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;


public class Database_Connection_OrientDB implements Database_Connection_Interface {
	private String dbDefaultDir_Windows = "C:/scratch/OrientDB/orientdb-graphed/databases/";
	private String dbDefaultDir_Linux = "/home/m113216/orient/databases/";
	private String dbDir = "";
	String dbName;
	OGraphDatabase orientDB;
	static final String USER = "admin";
	static final String PASS = "admin";
	
	Data_Common env;
	String [] randomRID_list;

	private HashSet<Object> GRAPH1, GRAPH2;
	ODocument vDoc;
	ODocument eDoc;
	
	public Database_Connection_OrientDB(Data_Common e){
		env = e;
		dbName = env.getDatabaseName();
		System.out.println("dbName = " + dbName);
		dbDir = dbDefaultDir_Windows;
		if(Data_Common.determineOS() == OperatingSystemType.LINUX){
			dbDir = dbDefaultDir_Linux;
		}
		GRAPH1 = new HashSet<Object>();
		GRAPH2 = new HashSet<Object>();
	}

	@Override
	public void createPrimaryKeys(){
		
	}
	
	@Override
	public void open() {
		File dir = new File(dbDir + dbName);
		if(!dir.exists()){
			System.out.println("Database does not exist");
			System.exit(1);
		}
		
		orientDB = new OGraphDatabase("local:" + dbDir + dbName);
		orientDB.open("admin",  "admin");
		vDoc = orientDB.createVertex();
		eDoc = orientDB.createEdge(vDoc, vDoc);
	}

	@Override
	public void delete() {
		File dir = new File(dbDir + dbName);
		if (dir.exists()) {
			System.out.println("exists,,,,, deleting");
			try {
				FileUtils.deleteDirectory(dir);
			} catch (Exception e) {
				System.out.println("Unable to delete old version of database: "	+ dbName);
			}
		}
	}

	@Override
	public void create() {
		delete();
		orientDB = new OGraphDatabase("local:" + dbDir + dbName);
		orientDB.create();
		orientDB.declareIntent(new OIntentMassiveInsert());
		vDoc = orientDB.createVertex();
		eDoc = orientDB.createEdge(vDoc, vDoc);
	}
	
	@Override
	public void close() {
		orientDB.close();
	}

	@Override
	public void setLargeInsert(){
		orientDB.declareIntent(new OIntentMassiveInsert());
	}
	
	@Override
	public void unsetLargeInsert(){
//		orientDB.declareIntent(new OIntentMassiveInsert());
	}
	
	@Override
	public String getDatabaseName(){
		return dbName;
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
			randomClusterName = env.getVertexPrefix() + (int) (Math.random() * env.getVertexTypeCount());
			clusterID = orientDB.getClusterIdByName(randomClusterName); 
			OClusterPosition [] range = orientDB.getStorage().getClusterDataRange(clusterID);
			
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
		results = orientDB.query(new OSQLSynchQuery<OIdentifiable>(sql));

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

	@Override
	public void createVertexTable(String table, String indexField,
			ArrayList<String> fieldnames) {
		createTable("vertex", table, indexField, fieldnames);
	}

	@Override
	public void createEdgeTable(String table, String indexField,
			ArrayList<String> fieldnames) {
		createTable("edge", table, indexField, fieldnames);
	}

	private void createTable(String type, String table, String indexField,
			ArrayList<String> fieldnames){
		OSchema schema = orientDB.getMetadata().getSchema();
		if(type.equals("edge")){
			orientDB.createEdgeType(table);
		}
		else{
			orientDB.createVertexType(table);
		}
		OClass oClass = schema.getClass(table);

		// Create ID field with unique index
		oClass.createProperty(indexField, OType.STRING);
		oClass.createProperty("label", OType.STRING);
		for (int j = 0; j < fieldnames.size(); j++) {
			oClass.createProperty(fieldnames.get(j), OType.STRING);
		}
		schema.save();		
	}
	
	@Override
	public Object storeVertex(String table, String idField, Long id,
			ArrayList<String> fieldnames, Scanner lineScan) {
		vDoc.reset();
		vDoc.setClassName(table);
		vDoc.getIdentity().reset();
		vDoc.field(idField, id);
		vDoc.field("label", table);
		for (String fieldName : fieldnames) {
			String value = lineScan.next();
			vDoc.field(fieldName, value);
		}

		vDoc.save();
		Object vID = vDoc.getIdentity().toString();

		return vID;
	}

	@Override
	public void storeEdges(File eFile,
			HashMap<Integer, ArrayList<String>> mapEdgeFieldNames,
			HashMap<Integer, ArrayList<Object>> mapVertexRIDs) {		
		try {
			Scanner scanner = new Scanner(eFile);
			int recordCount = 0;

			ODocument doc_in, doc_out;
			int tableID, in_tableID, in_recordID, out_tableID, out_recordID;

			long edgeID0 = 0;
			long edgeID1 = 10;

			String edgeID, line, id_in, id_out, idField;
			Scanner lineScan;
			String edgeTable, value;

			while (scanner.hasNextLine()) {
				if ((recordCount != 0) && ((recordCount % 10000) == 0)) {
					System.out.println("Working on the " + recordCount + "th edge");
				}
				edgeID = edgeID0 + "_" + edgeID1++;
							
				line = scanner.nextLine();
				lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				
				tableID = lineScan.nextInt();
				in_tableID = lineScan.nextInt();
				in_recordID = lineScan.nextInt();
				out_tableID = lineScan.nextInt();
				out_recordID = lineScan.nextInt();

				edgeTable = env.getEdgePrefix() + tableID;
				idField = edgeTable + env.getTableIdPrefix();

				id_in = (String) mapVertexRIDs.get(in_tableID).get(in_recordID);
				id_out = (String) mapVertexRIDs.get(out_tableID).get(out_recordID);
				
				doc_in = ((ArrayList<ODocument>) orientDB
						.query(new OSQLSynchQuery<ODocument>("Select from " + id_in))).get(0);
				doc_out = ((ArrayList<ODocument>) orientDB
						.query(new OSQLSynchQuery<ODocument>("Select from "	+ id_out))).get(0);

				eDoc.reset();
				eDoc.setClassName(env.getEdgePrefix() + tableID);
				eDoc.getIdentity().reset();

				eDoc.field("in", doc_in);
				eDoc.field("out", doc_out);

				eDoc.field(idField, edgeID);
				for (String fieldName : mapEdgeFieldNames.get(tableID)) {
					value = lineScan.next();
					eDoc.field(fieldName, value);
				}

				eDoc.save();
				String sql = "update " + doc_in.getIdentity() + " add out = "
						+ eDoc.getIdentity();
				orientDB.command(new OCommandSQL(sql)).execute();
				sql = "update " + doc_out.getIdentity() + " add in = "
						+ eDoc.getIdentity();
				orientDB.command(new OCommandSQL(sql)).execute();
				recordCount++;
			}
	
			scanner.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		// remove declareIntent
	}

	@Override
	public void createIndices() {
//		.setMandatory(true).setNotNull(true);
//oClass.createIndex(table + env.getTableIndexPrefix(),
//		OClass.INDEX_TYPE.UNIQUE, indexField);
	}

	@Override
	public void clearEdges() {
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
		System.out.println("Timing Iterator of " + iterations + " traversals of graph with " + env.getVertexCount() + " vertices");
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
		System.out.println("Average #records: " + (numRecords / iterations) + " of " + env.getVertexCount());
		System.out.println(String.format("Average Time: %d ms or (%d min, %d sec)", avgTime, minutes, seconds)); 
		System.out.println();					
	}
}
