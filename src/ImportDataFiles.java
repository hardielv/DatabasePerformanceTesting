import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class ImportDataFiles {
	DatabaseType DB_TYPE;

	DatabaseConnection_Interface db;

	HashMap<Integer, ArrayList<String>> mapVertexFieldNames;
	HashMap<Integer, ArrayList<String>> mapEdgeFieldNames;
	HashMap<Integer, ArrayList<Object>> mapVertexRIDs;
	GlobalEnvironment env;
	
	Boolean importAll = true;
	Boolean importEdges = true;
	Boolean createKeys = true;
	File vFile, eFile, vfFile, efFile;


	public void deleteDatabase() { db.delete(); }
	public void createDatabase() { db.create(); }
	public void closeDatabase() { db.close(); }
	public void clearEdges() { db.clearEdges(); }
	public void createPrimaryKeys() { db.createPrimaryKeys(); }
	public void createIndices() { db.createIndices(); }
	
	public void openFiles(){
		vFile = new File(env.VERTEX_FILE);
		vfFile = new File(env.VERTEX_FIELDS_PATH);
		efFile = new File(env.EDGE_FIELDS_PATH);		
	}

	public void openEdgeFile(){
		eFile = new File(env.EDGE_PATH);
	}
	

	public ImportDataFiles(GlobalEnvironment e) {
		env = e;
		mapVertexRIDs = new HashMap<Integer, ArrayList<Object>>();
		
		if(env.dbType == DatabaseType.ORIENTDB){
			db = new DatabaseConnection_OrientDB(env, databaseSizeToString(env.dbSize));
		}
		else{
			db = new DatabaseConnection_MySQL(env, databaseSizeToString(env.dbSize));
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
	


	public void storeMetaDataToDatabase() {

		// Read in vertex records from file
		try {
			Scanner scanner = new Scanner(vFile);
			mapVertexFieldNames = new HashMap<Integer, ArrayList<String>>();

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				Scanner lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				int typeID = lineScan.nextInt();
				mapVertexFieldNames.put(typeID, new ArrayList<String>());
				while (lineScan.hasNext()) {
					String value = lineScan.next();
					mapVertexFieldNames.get(typeID).add(value);
				}
			}

			scanner.close();
		} catch (FileNotFoundException e) {
			System.err.println("Error: " + e.getMessage());
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}

		// Read in edge records from file
		try {
			Scanner scanner = new Scanner(eFile);
			mapEdgeFieldNames = new HashMap<Integer, ArrayList<String>>();

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				Scanner lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				int typeID = lineScan.nextInt();
				mapEdgeFieldNames.put(typeID, new ArrayList<String>());
				while (lineScan.hasNext()) {
					String value = lineScan.next();
					mapEdgeFieldNames.get(typeID).add(value);
				}
			}

			scanner.close();
		} catch (FileNotFoundException e) {
			System.err.println("Error: " + e.getMessage());
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}

		// Create Tables in database
		String type, indexField;
		ArrayList<String> fieldnames;
		
		for (int i = 0; i < mapVertexFieldNames.size(); i++) {
			type = env.VERTEX_PREFIX + i;
			indexField = type + env.ID_PREFIX;

			fieldnames = mapVertexFieldNames.get(i);
			db.createVertexTable(type,  indexField,  fieldnames);
			
		}

		for (int i = 0; i < mapEdgeFieldNames.size(); i++) {
			type = env.EDGE_PREFIX + i;
			indexField = type + env.ID_PREFIX;

			fieldnames = mapEdgeFieldNames.get(i);
			db.createEdgeTable(type, indexField, fieldnames);			
		}
	}

	public void storeVerticesToDatabase() {
		Object vID;

		for (int i = 0; i < mapVertexFieldNames.size(); i++) {
			mapVertexRIDs.put(i, new ArrayList<Object>());
		}
		try {
			Scanner scanner = new Scanner(vFile);
			int recordCount = 0, typeID;
			// ODocument tDoc = db.createVertex(env.VERTEX_PREFIX + 0);
			String vertexType, idField;
			long vertexID1 = 10;
			long vertexID;
			ArrayList<String> fieldnames;
			while (scanner.hasNextLine()) {
				if (((recordCount++ + 1) % 10000) == 0) {
					System.out.println("Working on the " + recordCount
							+ "th vertex");
				}
				String line = scanner.nextLine();
				Scanner lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				typeID = lineScan.nextInt();

				vertexType = env.VERTEX_PREFIX + typeID;

				vertexID = vertexID1++;
				idField = env.VERTEX_PREFIX + typeID + env.ID_PREFIX;
				fieldnames = mapVertexFieldNames.get(typeID);
				
				vID = db.storeVertex(vertexType, idField, vertexID, fieldnames, lineScan);
				
				mapVertexRIDs.get(typeID).add(vID);
			}

			scanner.close();
		} catch (FileNotFoundException e) {
			System.err.println("(storeVertices) Error: " + e.getMessage());
		} catch (Exception e) {
			System.err.println("(storeVertices) Error: " + e.getMessage());
		}

	}

	public void storeEdgesToDatabase() {
		db.storeEdges(eFile, mapEdgeFieldNames, mapVertexRIDs);
	}
}
