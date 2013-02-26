import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

public class Database_SearchEngine {	
	public static void main(String[] args) {
		String resultsDir_Linux = "/home/m113216/scratch/";
		String resultsDir_Windows = "C:/scratch/";

		Database_Vendor [] dbVendors = Database_Vendor.values(); 
		int vendorIndex = 0;
		
		Database_QueryType [] queryList = Database_QueryType.values(); 
//		Database_QueryType [] queryList = {Database_QueryType.TRAVERSE};
		int [] queryIndices = {0, 1, 2, 3};
		
		String dir = null;
		ArrayList<String> databaseNames = new ArrayList<String>();
		if(args == null || args.length != 1){
			dir = Data_Common.getDefaultDatafilePath();
		} else {
			dir = args[0];
		}
		
		collectDatabaseNames(dir, databaseNames);
		
		int iterations = 10;
		int minDepth = 2, maxDepth = 10;
		
		Database_Search searchDB; 
		long memory = Runtime.getRuntime().totalMemory();
		Date date = new Date();
		
		Data_Common env;
		
		String resultsDir = resultsDir_Windows;
		if(Data_Common.determineOS() == OperatingSystemType.LINUX){
			resultsDir = resultsDir_Linux;
		}
			
		
		try{
			for(int i=0; i < databaseNames.size(); i++){
				System.out.println("-----------------------------");
				System.out.println("Connecting to " + databaseNames.get(i));
				env = new Data_Common(Data_Common.getDefaultDatafilePath() + databaseNames.get(i), databaseNames.get(i));
				env.build();
				boolean completed = true;
				for(int k=0; k < queryIndices.length; k++){
					for(int depth=minDepth; completed && depth <= maxDepth; depth++){
						searchDB = new Database_Search(env, dbVendors[vendorIndex], resultsDir);
						
						searchDB.print(date.toString(), memory, queryList[queryIndices[k]], iterations, depth);
						
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
	
	public static void collectDatabaseNames(String path, ArrayList<String> names){
		File file = new File(path);
		if(!file.exists()) return;

		String line;
		if(file.isDirectory()){
			for(File child: file.listFiles()){
				if(child.isDirectory()){
					names.add(child.getName());
				}
			}
		} else if(file.isFile()){
			try{
				Scanner readIN = new Scanner(file);
				while(readIN.hasNext()){
					line = readIN.next();
					if((line.length() > 1) && (!line.substring(0, 2).equals("//"))){
						names.add(line);
					}
				}	
				readIN.close();
			} catch(Exception e){
				System.err.println("Error: " + e.getMessage());
			}
		}
	}		
}
