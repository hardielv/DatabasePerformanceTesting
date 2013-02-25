import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class Database_Import {

	Database_Connection_Interface database;

	HashMap<Integer, ArrayList<String>> mapVertexFieldNames;
	HashMap<Integer, ArrayList<String>> mapEdgeFieldNames;
	HashMap<Integer, ArrayList<Object>> mapVertexRIDs;
	Data_Common env;
	
	String dataDir;
	File vFile, eFile, vfFile, efFile;

	public Database_Import(String dir, Database_Vendor dbVendor) {
		dataDir = dir + "/";
		System.out.println("dataDir = " + dataDir);
		env = new Data_Common(dataDir);
		env.build();
		mapVertexRIDs = new HashMap<Integer, ArrayList<Object>>();
		
		if(dbVendor == Database_Vendor.ORIENTDB){
			database = new Database_Connection_OrientDB(env);
		}
		else{
			database = new Database_Connection_MySQL(env);
		}
	}

	public void openDatabase() { database.open(); }
	public void deleteDatabase() { database.delete(); }
	public void createDatabase() { database.create(); }
	public void closeDatabase() { database.close(); }
	public void clearEdges() { database.clearEdges(); }
	public void createPrimaryKeys() { database.createPrimaryKeys(); }
	public void createIndices() { database.createIndices(); }
	
	public void openFiles(){
		vfFile = new File(dataDir + env.getVertexFieldsFile());
		efFile = new File(dataDir + env.getEdgeFieldsFile());		
		vFile = new File(dataDir + env.getVertexFile());
		eFile = new File(dataDir + env.getEdgeFile());
	}

	public void openEdgeFile(){
		eFile = new File(dataDir + env.getEdgeFile());
	}
	
	public void storeMetaDataToDatabase() {
		importMetaData();
		createTable(mapVertexFieldNames, env.getVertexPrefix(), "vertex");
		createTable(mapEdgeFieldNames, env.getEdgePrefix(), "edge");		
	}
	
	public void importMetaData(){
		mapVertexFieldNames = new HashMap<Integer, ArrayList<String>>();
		mapEdgeFieldNames = new HashMap<Integer, ArrayList<String>>();
		
		System.out.println(vfFile.toString());
		System.out.println(efFile.toString());
		
		readInFieldNames(mapVertexFieldNames, vfFile);
		readInFieldNames(mapEdgeFieldNames, efFile);		
	}
	
	public void readInFieldNames(HashMap<Integer, ArrayList<String>> map, File file) {
		// Read in vertex records from file
		try {
			Scanner scanner = new Scanner(file);
	
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				Scanner lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				int typeID = lineScan.nextInt();
				map.put(typeID, new ArrayList<String>());
				while (lineScan.hasNext()) {
					String value = lineScan.next();
					map.get(typeID).add(value);
				}
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	public void createTable(HashMap<Integer, ArrayList<String>> map, String prefix, String type){
		String tablename, indexField;
		ArrayList<String> fieldnames;
		for (int i = 0; i < map.size(); i++) {
			tablename = prefix + i;
			indexField = tablename + env.getTableIdPrefix();

			fieldnames = map.get(i);
			if(type.equals("edge")){
				database.createEdgeTable(tablename,  indexField,  fieldnames);
			}
			else{
				database.createVertexTable(tablename,  indexField, fieldnames);
			}
		}
	}

	public void storeVerticesToDatabase() {
		Object vID;

		database.setLargeInsert();
		
		for (int i = 0; i < mapVertexFieldNames.size(); i++) {
			mapVertexRIDs.put(i, new ArrayList<Object>());
		}
		try {
			Scanner scanner = new Scanner(vFile);
			int recordCount = 0, typeID;
			// ODocument tDoc = database.createVertex(env.VERTEX_PREFIX + 0);
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

				vertexType = env.getVertexPrefix() + typeID;

				vertexID = vertexID1++;
				idField = env.getVertexPrefix() + typeID + env.getTableIdPrefix();
				fieldnames = mapVertexFieldNames.get(typeID);
				
				vID = database.storeVertex(vertexType, idField, vertexID, fieldnames, lineScan);
				
				mapVertexRIDs.get(typeID).add(vID);
			}

			database.unsetLargeInsert();
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void storeEdgesToDatabase() {
		database.setLargeInsert();
		database.storeEdges(eFile, mapEdgeFieldNames, mapVertexRIDs);
		database.unsetLargeInsert();
	}
}
