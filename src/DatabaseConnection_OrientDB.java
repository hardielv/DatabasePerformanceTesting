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


public class DatabaseConnection_OrientDB implements DatabaseConnection_Interface {
	String DB_TYPE = "orientdb";
	String DB_PATH;
	String DB_NAME;
	OGraphDatabase db;
	static final String USER = "admin";
	static final String PASS = "admin";
	
	GlobalEnvironment env;
	String [] randomRID_list;

	private HashSet<Object> GRAPH1, GRAPH2;
	ODocument tDoc;
	
	
	public DatabaseConnection_OrientDB(GlobalEnvironment e, String size){
		env = e;
		DB_NAME = env.DB_NAME_PREFIX + size;
		DB_PATH = env.DB_PATH;
		GRAPH1 = new HashSet<Object>();
		GRAPH2 = new HashSet<Object>();
		open();
		tDoc = db.createVertex();
	}

	@Override
	public void createPrimaryKeys(){
		
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

	@Override
	public void delete() {
		File dir = new File(env.DB_PATH);
		if (dir.exists()) {
			try {
				FileUtils.deleteDirectory(dir);
			} catch (Exception e) {
				System.out.println("Unable to delete old version of database: "
						+ env.DB_PATH);
			}
		}
	}

	@Override
	public void create() {
		delete();
		db = new OGraphDatabase("local:" + env.DB_PATH);
		db.create();
	}

	@Override
	public void createVertexTable(String table, String indexField,
			ArrayList<String> fieldnames) {

		OSchema schema = db.getMetadata().getSchema();
		db.createVertexType(table);
		OClass oClass = schema.getClass(table);

		// Create ID field with unique index
		oClass.createProperty(indexField, OType.STRING)
				.setMandatory(true).setNotNull(true);
		oClass.createIndex(table + env.IDx_PREFIX,
				OClass.INDEX_TYPE.UNIQUE, indexField);
		oClass.createProperty("label", OType.STRING);
		// Create other fields
		for (int j = 0; j < fieldnames.size(); j++) {
			oClass.createProperty(fieldnames.get(j), OType.STRING);
		}
		schema.save();
	}

	@Override
	public void createEdgeTable(String table, String indexField,
			ArrayList<String> fieldnames) {
		OSchema schema = db.getMetadata().getSchema();
		db.createEdgeType(table);
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
		db.declareIntent(new OIntentMassiveInsert());
		
		tDoc.reset();
		tDoc.setClassName(table);
		tDoc.getIdentity().reset();
		tDoc.field(idField, id);
		tDoc.field("label", table);
		for (String fieldName : fieldnames) {
			String value = lineScan.next();
			tDoc.field(fieldName, value);
		}

		tDoc.save();
		Object vID = tDoc.getIdentity().toString();

		// remove declareIntent
		return vID;
	}

	@Override
	public void storeEdges(File eFile,
			HashMap<Integer, ArrayList<String>> mapEdgeFieldNames,
			HashMap<Integer, ArrayList<Object>> mapVertexRIDs) {
		
		db.declareIntent(new OIntentMassiveInsert());
		try {
			Scanner scanner = new Scanner(eFile);
			int recordCount = 0;

			ODocument doc_in, doc_out;
			ODocument tDoc = db.createEdge(new ODocument(), new ODocument(),
					env.EDGE_PREFIX + 0);
			int typeID, in_type, in_index, out_type, out_index;

			long edgeID0 = 0;
			long edgeID1 = 10;

			String edgeID, line, id_in, id_out, idField;
			Scanner lineScan;
			String edgeType, value;

			while (scanner.hasNextLine()) {
				recordCount++;
				line = scanner.nextLine();
				lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				typeID = lineScan.nextInt();
				in_type = lineScan.nextInt();
				in_index = lineScan.nextInt();
				out_type = lineScan.nextInt();
				out_index = lineScan.nextInt();
				id_in = (String) mapVertexRIDs.get(in_type).get(in_index);
				id_out = (String) mapVertexRIDs.get(out_type).get(out_index);
				doc_in = ((ArrayList<ODocument>) db
						.query(new OSQLSynchQuery<ODocument>("Select from "
								+ id_in))).get(0);
				doc_out = ((ArrayList<ODocument>) db
						.query(new OSQLSynchQuery<ODocument>("Select from "
								+ id_out))).get(0);

				tDoc.reset();
				tDoc.setClassName(env.EDGE_PREFIX + typeID);
				tDoc.getIdentity().reset();

				tDoc.field("in", doc_in);
				tDoc.field("out", doc_out);

				// Store ID field first
				edgeType = env.EDGE_PREFIX + typeID;
				idField = edgeType + env.ID_PREFIX;
				if (edgeID1 == 0) {
					edgeID1 = 10;
					edgeID0++;
				}

				edgeID = edgeID0 + "_" + edgeID1++;
				tDoc.field(idField, edgeID);

				if ((recordCount % 1000) == 0)
					db.commit();
				if ((recordCount % 10000) == 0) {
					System.out.println("Working on the " + recordCount
							+ "th edge");
				}

				for (String fieldName : mapEdgeFieldNames.get(typeID)) {
					value = lineScan.next();
					tDoc.field(fieldName, value);
				}

				tDoc.save();
				String sql = "update " + doc_in.getIdentity() + " add out = "
						+ tDoc.getIdentity();
				db.command(new OCommandSQL(sql)).execute();
				sql = "update " + doc_out.getIdentity() + " add in = "
						+ tDoc.getIdentity();
				db.command(new OCommandSQL(sql)).execute();
			}

			scanner.close();

		} catch (FileNotFoundException e) {
			System.err.println("(storeEdge) Error: " + e.getMessage());
		} catch (Exception e) {
			System.err.println("(storeEdge) Error: " + e.getMessage());
		}
		// remove declareIntent
	}

	@Override
	public void createIndices() {
	}

	@Override
	public void clearEdges() {
		// TODO Auto-generated method stub
		
	}
}
