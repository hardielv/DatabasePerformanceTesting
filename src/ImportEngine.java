
public class ImportEngine {
//	final static DatabaseType [] dbTypes = {DatabaseType.MYSQL};
	final static DatabaseType [] dbTypes = {DatabaseType.ORIENTDB};
	
//	final static DatabaseSize [] size = {DatabaseSize.SMALL, DatabaseSize.MEDIUM, DatabaseSize.LARGE, DatabaseSize.HUGE}; 
	final static DatabaseSize [] size = {DatabaseSize.SMALL}; 

	final static OperatingSystemType osType = OperatingSystemType.WINDOWS;
//	final static OperatingSystemType osType = OperatingSystemType.LINUX;
	
	public static void main(String[] args) {
		ImportDataFiles importDB;
		GlobalEnvironment env;
		
		for(int i=0; i < size.length; i++){
			System.out.println("-----------------------------");
			System.out.println("Connecting to " + size[i]);

			env = new GlobalEnvironment(osType, dbTypes[0], size[i]);
			importDB = new ImportDataFiles(env);

		if (importDB.importAll) {
			System.out.println("deleting database");
			importDB.deleteDatabase();
			System.out.println("Re-creating database");
			importDB.createDatabase();
			importDB.openFiles();
			
			System.out.println("Storing Metadata");
			importDB.storeMetaDataToDatabase();
			// Save data to database
			System.out.println("Storing Vertices");
			importDB.storeVerticesToDatabase();
		} else if (importDB.importEdges) {
			System.out.println("Clearing out edge tables");
			importDB.clearEdges();
		}

		if (importDB.importEdges) {
			System.out.println("Storing Edges");
			importDB.openEdgeFile();
			importDB.storeEdgesToDatabase();
		}

		if (importDB.createKeys) {
			System.out.println("Creating Primary Keys");
			importDB.createPrimaryKeys();
			System.out.println("Creating Indices");
			importDB.createIndices();
		}

		importDB.closeDatabase();
		// importDB.printEdges();
		// importDB.storeEdgesSQLtoFile(eFile);
		System.out.println("Finished");
		}

	}
}
