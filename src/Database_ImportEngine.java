import java.io.File;


public class Database_ImportEngine {
	public static void main(String[] args) {
		boolean importAll = true;
		boolean importEdges = true;
		boolean createKeys = false;
		
		String dataPrefix = "randomDB_";

//		DatabaseType [] dbTypes = {DatabaseType.MYSQL};
		Database_Vendor [] dbVendors = {Database_Vendor.ORIENTDB};

		Database_Import importDB;
		String datafilePath = Data_Common.getDefaultDatafilePath();
		String dataName;
		
		File dataDir = new File(datafilePath);
		if(dataDir.exists() && dataDir.isDirectory()){
			File [] randomDirs = dataDir.listFiles();
			for(int i=0; i < randomDirs.length; i++){
				if(randomDirs[i].isDirectory()){
					dataName = randomDirs[i].getName();
					System.out.println("Connecting to " + dataName);
					System.out.println("-----------------------------");
					System.out.println(randomDirs[i].getAbsolutePath());
					
					for(int j=0; j < dbVendors.length; j++){
						importDB = new Database_Import(datafilePath + dataName, dbVendors[j]);

						importDB.openFiles();
						
						if (importAll) {
							importDB.deleteDatabase();
							importDB.createDatabase();
							importDB.storeMetaDataToDatabase();
							importDB.storeVerticesToDatabase();
						} else if (importEdges) {
							importDB.openDatabase();
							importDB.clearEdges();
						}
				
						if (importEdges) {
							System.out.println("Storing Edges");
							importDB.importMetaData();
							importDB.storeEdgesToDatabase();
						}
				
						if (createKeys) {
							System.out.println("Creating Primary Keys");
							importDB.createPrimaryKeys();
							System.out.println("Creating Indices");
							importDB.createIndices();
						}
				
						importDB.closeDatabase();
					}			
					System.out.println("Finished this size");
				}
			}
		}
		
		System.out.println("All Done");
	}
	
	
}
	