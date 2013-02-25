import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Scanner;


public class Data_Common {
	private final static String default_Linux_datafilePath = "/home/m113216/datafiles/";	
	private final static String default_Windows_datafilePath = "C:/scratch/datafiles/";
	
	private String DATA_DIR;
	private String SPECS_FILE = "dataSpecs.txt";
	private String SPECS_FULL_PATH;
	private String DB_NAME; 		
	
	private String VERTEX_FILE = "vertexData.odb"; 
	private String EDGE_FILE = "edgeData.odb";
	private String VERTEX_FIELDS_FILE = "vertexfieldData.odb";
	private String EDGE_FIELDS_FILE = "edgefieldData.odb";
		
	private String VERTEX_PREFIX = "Vertex_";
	private String EDGE_PREFIX = "Edge_";
	private String TABLE_ID_PREFIX = "_ID";
	private String TABLE_IDx_PREFIX = "_IDx";
	
	private int NUM_VERTEX_TYPES, NUM_EDGE_TYPES;
	private int TOTAL_VERTICES, TOTAL_EDGES;	
	
	public Data_Common(String datafilePath) {
		DATA_DIR = datafilePath;
		SPECS_FULL_PATH = DATA_DIR + "/" + SPECS_FILE;
	}
	
	public Data_Common(String datafilePath, String dbName) {
		DATA_DIR = datafilePath;
		SPECS_FULL_PATH = DATA_DIR + "/" + SPECS_FILE;
		DB_NAME = dbName;
	}
	
	public void build(){
		File envFile = new File(SPECS_FULL_PATH);
		
		try{
			String line, field;
			Scanner scanner = new Scanner(envFile);
			while(scanner.hasNextLine()){
				line = scanner.nextLine();
				Scanner lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				field = lineScan.next();
				
				if(field.equals("DATA_DIR")) DATA_DIR = lineScan.next();
				else if(field.equals("SPECS_FILE")) SPECS_FILE = lineScan.next();
				else if(field.equals("DB_NAME")) DB_NAME = lineScan.next();		
				
				else if(field.equals("VERTEX_FILE")) VERTEX_FILE = lineScan.next(); 
				else if(field.equals("EDGE_FILE")) EDGE_FILE = lineScan.next();
				else if(field.equals("VERTEX_FIELDS_FILE")) VERTEX_FIELDS_FILE = lineScan.next();
				else if(field.equals("EDGE_FIELDS_FILE")) EDGE_FIELDS_FILE = lineScan.next();
					
				else if(field.equals("VERTEX_PREFIX")) VERTEX_PREFIX = lineScan.next();
				else if(field.equals("EDGE_PREFIX")) EDGE_PREFIX = lineScan.next();
				else if(field.equals("TABLE_ID_PREFIX")) TABLE_ID_PREFIX = lineScan.next();
				else if(field.equals("TABLE_IDx_PREFIX")) TABLE_IDx_PREFIX = lineScan.next();
				
				else if(field.equals("NUM_VERTEX_TYPES")) NUM_VERTEX_TYPES = lineScan.nextInt();
				else if(field.equals("NUM_EDGE_TYPES")) NUM_EDGE_TYPES = lineScan.nextInt();
				else if(field.equals("TOTAL_VERTICES")) TOTAL_VERTICES = lineScan.nextInt();
				else if(field.equals("TOTAL_EDGES")) TOTAL_EDGES = lineScan.nextInt();	
				lineScan.close();
			}			
			scanner.close();
		} catch(Exception e){
			System.err.println("Error: " + e.getMessage());
		}
	}
	
	
	public void save(String dbName, String path, Data_Specs specs, int vTypeCount, int eTypeCount){		
		TOTAL_VERTICES = specs.getVertexCount();
		TOTAL_EDGES    = specs.getEdgeCount();	

		NUM_VERTEX_TYPES = vTypeCount;
		NUM_EDGE_TYPES = eTypeCount;
		
		String filename = path + "/" + SPECS_FILE;
		File specFile = new File(filename);
		if(specFile.exists()) specFile.delete();
		try{
			FileWriter fstream = new FileWriter(specFile);
			BufferedWriter out = new BufferedWriter(fstream);
			
			out.write("DATA_DIR," + DATA_DIR + "\n");
			out.write("SPECS_FILE," + SPECS_FILE + "\n");
			out.write("DB_NAME," + dbName + "\n");		
			
			out.write("VERTEX_FILE," + VERTEX_FILE + "\n"); 
			out.write("EDGE_FILE," + EDGE_FILE + "\n");
			out.write("VERTEX_FIELDS_FILE," + VERTEX_FIELDS_FILE + "\n");
			out.write("EDGE_FIELDS_FILE," + EDGE_FIELDS_FILE + "\n");
				
			out.write("VERTEX_PREFIX," + VERTEX_PREFIX + "\n");
			out.write("EDGE_PREFIX," + EDGE_PREFIX + "\n");
			out.write("TABLE_ID_PREFIX," + TABLE_ID_PREFIX + "\n");
			out.write("TABLE_IDx_PREFIX," + TABLE_IDx_PREFIX + "\n");
							
			out.write("NUM_VERTEX_TYPES," + NUM_VERTEX_TYPES + "\n");
			out.write("NUM_EDGE_TYPES," + NUM_EDGE_TYPES + "\n");
			out.write("TOTAL_VERTICES," + TOTAL_VERTICES + "\n");
			out.write("TOTAL_EDGES," + TOTAL_EDGES + "\n");	
			
			out.flush();
			out.close();
		} catch(Exception e){
			System.err.println("Error: " + e.getMessage());
		}

	}
	
	
	public static String getDefaultDatafilePath(OperatingSystemType osType){
		if(osType == OperatingSystemType.LINUX)
			return default_Linux_datafilePath;
		
		return default_Windows_datafilePath;
	}

	public String getDataDir() { return DATA_DIR; }

	public String getVertexFile(){ return VERTEX_FILE;} 
	public String getEdgeFile(){ return EDGE_FILE;}
	public String getVertexFieldsFile(){ return VERTEX_FIELDS_FILE;}
	public String getEdgeFieldsFile(){ return EDGE_FIELDS_FILE;}
	
	public String getVertexPrefix(){ return VERTEX_PREFIX;}
	public String getEdgePrefix(){ return EDGE_PREFIX;}
	public String getTableIdPrefix(){ return TABLE_ID_PREFIX;}
	public String getTableIndexPrefix(){ return TABLE_IDx_PREFIX;}
	public String getDatabaseName() { return DB_NAME; }
	public int getVertexTypeCount() { return NUM_VERTEX_TYPES;}
	public int getEdgeTypeCount() { return NUM_EDGE_TYPES;}
	public int getVertexCount() { return TOTAL_VERTICES;}
	public int getEdgeCount() { return TOTAL_EDGES;}	
}
