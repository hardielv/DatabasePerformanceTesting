import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;

public class Database_Connection_MySQL implements Database_Connection_Interface {
	// DATABASE TYPE SPECIFIC
	String DB_TYPE = "mysql";
	String ENV_DIR;
	String DB_NAME;

	final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   	final String DB_URL = "jdbc:mysql://localhost:3307/";
   	Connection db;
   	Statement stmt;
	//  Database credentials
	static final String USER = "lexgrid";
	static final String PASS = "lexgrid";
	
	Data_Common env;
	Long [] randomRID_list;
   	
	private HashSet<Object> GRAPH1, GRAPH2;
	

	public Database_Connection_MySQL(Data_Common e){
		env = e;
		DB_NAME = env.getDatabaseName();
		ENV_DIR = env.getDataDir();
		GRAPH1 = new HashSet<Object>();
		GRAPH2 = new HashSet<Object>();
		open();
	}
	
	@Override
	public void createPrimaryKeys() {
		String sql = "USE " + DB_NAME;
		try {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		createPrimaryKeys(env.getVertexPrefix(), env.getVertexTypeCount());
		createPrimaryKeys(env.getEdgePrefix(), env.getEdgeTypeCount());
	}

	private void createPrimaryKeys(String table, int numTables) {
		String sql, tableName;
		for (int i = 0; i < numTables; i++) {
			tableName = table + i;
			sql = "alter table " + tableName + " add primary key (" + tableName
					+ "_ID)";
			System.out.println(sql);
			try {
				stmt = db.createStatement();
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void createIndices() {
		String sql = "USE " + DB_NAME;
		try {
			stmt.executeUpdate(sql);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		String tableName;
		for (int i = 0; i < env.getEdgeTypeCount(); i++) {
			tableName = env.getEdgePrefix() + i;
			// sql = "alter table " + tableName + " add unique idx_" + tableName
			// + " (edgeIN, edgeOUT)";
			sql = "create index idx_" + tableName + " on " + tableName
					+ " (edgeIN, edgeOUT)";
			System.out.println(sql);
			try {
				stmt = db.createStatement();
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void clearEdges() {
		String sql;
		for (int i = 0; i < env.getEdgeTypeCount(); i++) {
			sql = "Delete from " + env.getEdgePrefix() + i;
			try {
				stmt = db.createStatement();
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void delete() {
		try {
			String sql = "DROP DATABASE " + DB_NAME;
			System.out.println(sql);
			stmt.executeUpdate(sql);
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void create() {
		try {
			String sql = "CREATE DATABASE " + DB_NAME;
			stmt.executeUpdate(sql);
			sql = "USE " + DB_NAME;
			stmt.executeUpdate(sql);
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void open() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			db = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = db.createStatement();
			stmt.executeUpdate("USE " + DB_NAME);
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (ClassNotFoundException ce) {
			ce.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
			stmt.close();
			db.close();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setLargeInsert(){
//		db.declareIntent(new OIntentMassiveInsert());
	}
	
	@Override
	public void unsetLargeInsert(){
//		db.declareIntent(new OIntentMassiveInsert());
	}
	
	@Override
	public String getDatabaseName() {
		return DB_NAME;
	}
	
	@Override
	public void createVertexTable(String table, String indexField, ArrayList<String> fieldnames) {
		String sql;
		
		sql = "CREATE TABLE " + table;
		// Create ID field with unique index
		// sql += " (" + vType_IndexField +
		// " VARCHAR(255) NOT NULL PRIMARY KEY, label VARCHAR(255)";
		sql += " (" + indexField
				+ " BIGINT NOT NULL, label VARCHAR(255)";
		// TODO: Create index
		// Create other fields
		for (int j = 0; j < fieldnames.size(); j++) {
			sql += ", " + fieldnames.get(j) + " VARCHAR(255)";
		}
		sql += ")";
		System.out.println(sql);
		try {
			stmt = db.createStatement();
			stmt.executeUpdate(sql);
		} catch (SQLException se) {
			se.printStackTrace();
		}
	}

	@Override
	public void createEdgeTable(String table, String indexField, ArrayList<String> fieldnames) {
		String sql;
		sql = "CREATE TABLE " + table;
		// Create ID field with unique index
		// sql += " (" + eType_IndexField +
		// " VARCHAR(255) NOT NULL PRIMARY KEY, label VARCHAR(255), edgeIN VARCHAR(255), edgeOUT VARCHAR(255)";
		sql += " ("
				+ indexField
				+ " BIGINT NOT NULL, label VARCHAR(255), edgeIN VARCHAR(255), edgeOUT VARCHAR(255)";
		// TODO: Create index
		// Create other fields
		for (int j = 0; j < fieldnames.size(); j++) {
			sql += ", " + fieldnames.get(j) + " VARCHAR(255)";
		}
		sql += ")";
		System.out.println(sql);
		try {
			stmt = db.createStatement();
			stmt.executeUpdate(sql);
		} catch (SQLException se) {
			se.printStackTrace();
		}
		

	}
	
	@Override
	public Object storeVertex(String table, String idField, Long id,
			ArrayList<String> fieldnames, Scanner lineScan) {
		String sql, sqlFields, sqlValues;
		sqlFields = idField + ", label";
		sqlValues = "'" + id + "', '" + table + "'";
		// Store fields from file...
		for (String fieldName : fieldnames) {
			String value = lineScan.next();
			// tDoc.field(fieldName, value);
			sqlFields += ", " + fieldName;
			sqlValues += ", '" + value + "'";
		}

		sql = "INSERT INTO " + table + " (" + sqlFields
				+ ") VALUES (" + sqlValues + ")";
		try {
			stmt = db.createStatement();
			stmt.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return id;
	}
	
	@Override
	public void storeEdges(File eFile, 
			HashMap<Integer, ArrayList<String>> mapEdgeFieldNames, 
			HashMap<Integer, ArrayList<Object>> mapVertexRIDs){
		int typeID, in_type, in_index, out_type, out_index;
		long edgeID = 10, id_in, id_out;
		String idField, line, edgeType, sql, sqlFields, sqlValues;
		Scanner lineScan;

		PreparedStatement[] pStatements = new PreparedStatement[env.getEdgeTypeCount()];
		int[] recordCount = new int[env.getEdgeTypeCount()];
		long totalRecords = 0;

		// Create sql statements and save in prepared statements array
		for (int i = 0; i < pStatements.length; i++) {
			recordCount[i] = 0;
			edgeType = env.getEdgePrefix() + i;
			idField = edgeType + env.getTableIdPrefix();
			// Store fields from file...
			sqlFields = idField + ", label, edgeIN, edgeOUT";
			sqlValues = "?, ?, ?, ?";

			for (String fieldName : mapEdgeFieldNames.get(i)) {
				sqlFields += ", " + fieldName;
				sqlValues += ", ?";
			}

			sql = "INSERT INTO " + edgeType + " (" + sqlFields + ") VALUES ("
					+ sqlValues + ")";
			System.out.println(sql);
			try {
				pStatements[i] = db.prepareStatement(sql);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		// Pull data from import file and add to prepared statement batch
		// method.
		try {
			Scanner scanner = new Scanner(eFile);

			while (scanner.hasNextLine()) {
				line = scanner.nextLine();
				lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				typeID = lineScan.nextInt();
				in_type = lineScan.nextInt();
				in_index = lineScan.nextInt();
				out_type = lineScan.nextInt();
				out_index = lineScan.nextInt();
				id_in = (Long) mapVertexRIDs.get(in_type).get(in_index);
				id_out = (Long) mapVertexRIDs.get(out_type).get(out_index);

				edgeType = env.getEdgePrefix() + typeID;
				edgeID++;
				int index = 1;
				totalRecords++;
				recordCount[typeID]++;
				pStatements[typeID].setLong(index++, edgeID); // ID
				pStatements[typeID].setString(index++, edgeType); // label
				pStatements[typeID].setLong(index++, id_in); // edgeIN
				pStatements[typeID].setLong(index++, id_out); // edgeOUT

				// Store fields from file...
				int numValues = mapEdgeFieldNames.get(typeID).size();
				for (int i = 0; i < numValues; i++) {
					String value2 = lineScan.next();
					pStatements[typeID].setString(index++, value2); // fieldValue
				}

				pStatements[typeID].addBatch();
				// System.out.println(sql);
				// stmt = db.createStatement();
				// stmt.execute(sql);
				//
				// execute batch update to database every 1,000 records
				// Notify user progress every 10,000 records
				for (int i = 0; i < recordCount.length; i++) {
					if ((recordCount[i] + 1) % 1000 == 0) {
						// System.out.println(totalRecords +
						// " -- Commiting records to " + env.getEdgePrefix() + i +
						// ", " + recordCount[i]);
						pStatements[i].executeBatch();
					}
					if ((totalRecords + 1) % 100000 == 0) {
						System.out.println(totalRecords + 1);
					}
				}
			}

			System.out.println("Executing final batches...");
			for (int i = 0; i < pStatements.length; i++) {
				pStatements[i].executeBatch();
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			System.err.println("(storeEdge) Error: " + e.getMessage());
		} catch (Exception e) {
			System.err.println("(storeEdge) Error: " + e.getMessage());
		}
	}
	
	
	@Override
	public Object getRandomRID(int i) {
		return randomRID_list[i];
	}

	@Override
	public void setRandomRID(int i, Object value) {
		randomRID_list[i] = (Long) value;
	}

	@Override
	public void collectRandomRIDs(int iterations) {
		// Add #iterations plus one for the set operations to have two subgraphs per iteration
		int numRIDs = iterations + 1;
		randomRID_list = new Long[numRIDs];
		String randomTableName, primaryKey, sql;
		ResultSet idRange;
		long randomPosition;
		long minID, maxID;
		// Collect #iterations of random RID's
		for(int i=0; i < numRIDs; i++){
			randomTableName = env.getVertexPrefix() + (int) (Math.random() * env.getVertexTypeCount());
			primaryKey = randomTableName + "_ID";
			try {
				stmt = db.createStatement();
				sql = "Select min(" + primaryKey + ") as minID, max(" + primaryKey + ") as maxID from " + randomTableName;
				idRange = stmt.executeQuery(sql);
								
				if(idRange.next()){
					minID = idRange.getInt("minID");
					maxID = idRange.getInt("maxID");
					
					randomPosition = (long) (Math.random() * (maxID)) + minID;
					randomRID_list[i] = randomPosition;
				}
				else{
					System.out.println("No min/max values returned");
				}
				
				idRange.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public HashSet<Object> traverseJava(Object root, int level, int depth, int graphID){
		String sql;
		Long id;
		if (graphID == 1) {
			GRAPH1.add(root);
		} else {
			GRAPH2.add(root);
		}
		if (level <= depth) {
			sql = "Select edgeOUT from tempEdges where edgeIN = " + root;
			Statement stmt2;
			try {
				stmt2 = db.createStatement();
				ResultSet resultSet = stmt2.executeQuery(sql);
				while (resultSet.next()) {
					id = resultSet.getLong("edgeOUT");
					traverseJava(id, level + 1, depth, graphID);
				}
				resultSet.close();
				stmt2.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
	public void printRecords(List<Object> recList) {
		// TODO Auto-generated method stub
		
	}

	// ------------------------------------------------------------
	
	private String createSQL_Traverse(long id, int depth){
		String sql = "select ";
		
		String sqlFields = "", sqlTables = "", sqlWhere = "", sqlOrder = "";
		String comma = "";
		for(int i=1; i <= depth; i++){
			sqlFields += comma + "L" + i + ".edgeOUT as L" + i + "_OUT, L" + i + ".edgeIN as L" + i + "_IN";
			sqlTables += comma + "tempEdges as L" + i;
			sqlOrder += comma + "L" + i + ".edgeOUT";
			if(i>1){
				sqlWhere += "L" + (i-1) + ".edgeIN = L" + i + ".edgeOUT and ";
			}
			comma = ", ";
		}
		
		sql = "Select " + sqlFields + " from " + sqlTables + " where " + sqlWhere + " L1.edgeOUT = " + id;
		sql += " order by " + sqlOrder;
		return sql;
	}
}
