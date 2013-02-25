import java.util.Date;

public class Database_SearchEngine {	
	public static void main(String[] args) {
		OperatingSystemType osType = OperatingSystemType.WINDOWS;
//		OperatingSystemType osType = OperatingSystemType.LINUX;
		
//		DatabaseType [] dbTypes = {DatabaseType.MYSQL};
		Database_Vendor [] dbTypes = {Database_Vendor.ORIENTDB};
		
		Data_Size [] size = {Data_Size.SMALL, Data_Size.MEDIUM, Data_Size.LARGE, Data_Size.HUGE}; 
//		DatabaseSize [] size = {DatabaseSize.SMALL}; 
		
		Database_QueryType [] queryList = {Database_QueryType.TRAVERSE, Database_QueryType.INTERSECTION, Database_QueryType.UNION, Database_QueryType.DIFFERENCE};
//		QueryType [] queryList = {QueryType.TRAVERSE};
		
		int iterations = 1;
		int minDepth = 2, maxDepth = 2;
		
		Database_Search searchDB; 
		long memory = Runtime.getRuntime().totalMemory();
		Date date = new Date();
		
		Data_Common env;
		String dbPath = Data_Common.getDefaultDatafilePath(osType);
		
		try{
			for(int i=0; i < size.length; i++){
				System.out.println("-----------------------------");
				System.out.println("Connecting to " + size[i]);
				env = new Data_Common(dbPath);
				boolean completed = true;
				for(int k=0; k < queryList.length; k++){
					for(int depth=minDepth; completed && depth <= maxDepth; depth++){
						searchDB = new Database_Search(env);
						
						searchDB.print(date.toString(), memory, size[i], queryList[k], iterations, depth);
						
						searchDB.openDatabase();
						searchDB.collectRandomRIDs(iterations);
						completed = searchDB.timePerformance(queryList[k], iterations, depth);
	
						searchDB.closeDatabase();
						searchDB.closeFiles();
					}
					System.out.println();
				}
			}
		} catch(Exception e){
			System.out.println("Some sort of error occurred.");
			e.printStackTrace();
		}
		System.out.println("Done");
	}	
}
