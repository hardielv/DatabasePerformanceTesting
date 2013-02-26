import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;


public class GenerateRandomDataFiles {
	private Data_Common env;
	private Data_Specs dataSpecs;
	
	private String dbName;
	private int vertexTypeCount;
	private int edgeTypeCount;
	
	private String [][] listVertexFieldNames;
	private String [][] listEdgeFieldNames;
	
	private HashMap<Integer, ArrayList<Data_Vertex>> mapVertices;
	private HashMap<Integer, ArrayList<Data_Edge>> mapEdges;
	
	private Scanner s; 
	private boolean randomTypeCounts;
	
	public static void main(String[] args) {
		boolean print = false;
		boolean randomTypeCounts = false;
		String dbPrefix = "randomDB_";
		int vertexTypes = 200;
		int edgeTypes = 20;
		int vertexFields = 4;
		int edgeFields = 3;
		
		Data_Size [] dbAllSizes = Data_Size.values();
		Data_MaxValues maxValues = new Data_MaxValues(vertexTypes, edgeTypes, vertexFields, edgeFields);

		// List of indices of corresponding data_sizes to generate
		int dbToCreate[] = {4};
//		int dbToCreate[] = {0, 1, 2, 3, 4};  // ALL

		String dbNamePrefix, exportDir;
		int sizeIndex, vCount;
		GenerateRandomDataFiles randomDB;
		Data_Specs data;
		for(int i=0; i < dbToCreate.length; i++){
			sizeIndex = dbToCreate[i];
			dbNamePrefix = dbPrefix + dbAllSizes[sizeIndex].toString();
			exportDir = Data_Common.getDefaultDatafilePath();
			
			vCount = dbAllSizes[sizeIndex].getVertexCount();
			System.out.println("Genering datafiles for " + vCount + " vertices...");
			data = new Data_Specs(dbAllSizes[sizeIndex], maxValues);
			randomDB = new GenerateRandomDataFiles(data, dbNamePrefix, exportDir, randomTypeCounts);
			if(randomDB.createDirectory()){
				randomDB.createGraph();
				if(print) randomDB.print();
			}
		}
		
		System.out.println("DONE");
	}	

	public GenerateRandomDataFiles(Data_Specs specs, String name, String datafileDir, boolean randomCounts){
		dataSpecs = specs;
		dbName = name;
		randomTypeCounts = randomCounts;
		env = new Data_Common(datafileDir);
		s = new Scanner(System.in);
		
		
		System.out.print("maxVertexTypeCount: " + dataSpecs.getMaxVertexTypes() + ", ");
		System.out.println("maxEdgeTypeCount: " + dataSpecs.getMaxEdgeTypes());
		
		vertexTypeCount = dataSpecs.getMaxVertexTypes();
		edgeTypeCount = dataSpecs.getMaxEdgeTypes();
		if(randomTypeCounts){
			vertexTypeCount = (int) (Math.random() * dataSpecs.getMaxVertexTypes()) + 1;
			edgeTypeCount = (int) (Math.random() * dataSpecs.getMaxEdgeTypes()) + 1;
		}
		
		dbName += "_" + vertexTypeCount + "_" + edgeTypeCount;
		System.out.println(dbName);
		System.out.println("vertexTypeCount : " + vertexTypeCount + ", edgeTypeCount : " + edgeTypeCount);
		listVertexFieldNames = new String[vertexTypeCount][];
		listEdgeFieldNames = new String[edgeTypeCount][];
		
		// Configure random environment to create Vertices and Edges
		createFields("vertex", listVertexFieldNames, dataSpecs.getMaxVertexFields());
		createFields("edge", listEdgeFieldNames, dataSpecs.getMaxEdgeFields());
		
		createVertexMap();
		createEdgeMap();	
	}

	public boolean createDirectory(){
		boolean success;
		String path = env.getDataDir() + dbName;
		System.out.println("creating directory: " + path);
		File dir = new File(path);
		success = dir.mkdirs();
		return success;
	}
	private void createVertexMap(){
		mapVertices = new HashMap<Integer, ArrayList<Data_Vertex>>();
		for(int i=0; i < vertexTypeCount; i++){
			mapVertices.put(i, new ArrayList<Data_Vertex>());
		}
	}
	
	private void createEdgeMap(){
		mapEdges = new HashMap<Integer, ArrayList<Data_Edge>>();
		for(int i=0; i < edgeTypeCount; i++){
			mapEdges.put(i, new ArrayList<Data_Edge>());
		}
	}

	private void clearEdgeMap(){
		System.out.println("Clearing edge map");
		for(int i=0; i < edgeTypeCount; i++){
			mapEdges.get(i).clear();
		}
	}
	
	public void createGraph(){
		String path = env.getDataDir() + "/" + dbName;
		System.out.println("in createGraph, path = " + path);
		saveEnvironment(path);
		createVertices(path);
//		storeVertices(path);
		createEdges(path);
//		storeEdges(path);
	}
	
	public void saveEnvironment(String path){
		env.save(dbName, path, dataSpecs, vertexTypeCount, edgeTypeCount);
	}
	
	
	private void createFields(String type, String[][] groups, int max){
		for(int i=0; i < groups.length; i++){
			int size = (int) (Math.random() * max) + 1;
			groups[i] = new String[size];
			for(int j=0; j < size; j++){
				groups[i][j] = type + i + "_field" + j;
			}			
		}		
	}
	

	public void createVertices(String path){
		System.out.println("Creating " + dataSpecs.getVertexCount() + " vertices");
//		storeVertices(path, true);
		
		for(int i=0; i < dataSpecs.getVertexCount(); i++){
//			if((i != 0) && ((i % 1000) == 0)){
//				System.out.println("Created " + i + " nodes of " + dataSpecs.getVertexCount());
//				storeVertices(path, false);
////				clearVertexMap();
//			}
			
			Integer vertexTypeID = (int) (Math.random() * vertexTypeCount);
			Data_Vertex v = new Data_Vertex(env.getVertexPrefix() + vertexTypeID, listVertexFieldNames[vertexTypeID].length);
			
			// Generate random values, one for each field in given vertex
			for(int j=0; j < v.getSize(); j++){
				int value = (int) (Math.random() * dataSpecs.getMaxValue());
				v.addValue(j, "value_" + value);
			}
			// Insert into list of vertices
			mapVertices.get(vertexTypeID).add(v);
		}
		storeVertices(path, true);
	}

	public void createEdges(String path){
		System.out.println("Creating " + dataSpecs.getEdgeCount() + " edges");
		int edgeTypeID;
		int [] v_in = new int[2];
		int [] v_out = new int[2];
		Data_Edge edge;
		storeEdges(path, true);		
		
		
		for(int i = 0; i < dataSpecs.getEdgeCount(); i++){
			if((i !=0) && ((i % 10000) == 0)){
				System.out.println("Created " + i + " edges of " + dataSpecs.getEdgeCount());
				storeEdges(path, false);
				clearEdgeMap();
			}

			v_in[0] = (int) (Math.random() * vertexTypeCount);
			v_out[0] = (int) (Math.random() * vertexTypeCount);
			
			v_in[1] = (int) (Math.random() * mapVertices.get(v_in[0]).size());
			v_out[1] = (int) (Math.random() * mapVertices.get(v_out[0]).size()); 
			
			// Infinite loop if one vertexType and one Vertex
			while((v_in[0] == v_out[0]) && (v_in[1] == v_out[1])){
				v_out[0] = (int) (Math.random() * vertexTypeCount);
				v_out[1] = (int) (Math.random() * mapVertices.get(v_out[0]).size());				
			}
	
			edgeTypeID = (int)(Math.random() * edgeTypeCount);
			edge = new Data_Edge(env.getEdgePrefix() + edgeTypeID, listEdgeFieldNames[edgeTypeID].length);
			edge.connect(v_in, v_out);
			
			for(int j=0; j < edge.getSize(); j++){
				int value = (int) (Math.random() * dataSpecs.getMaxValue());
				edge.addValue(j, "value_" + value);
			}			
			
			mapEdges.get(edgeTypeID).add(edge);
		}		
		storeEdges(path, false);		
	}
	
	public void printVertices(){
		System.out.println("Vertices...");
		for(int i=0; i < vertexTypeCount; i++){
			ArrayList<Data_Vertex> vertices = mapVertices.get(i);
			int vertexIndex=0;
			for(Data_Vertex v:vertices){
				System.out.print(v.getType() + "_" + vertexIndex++ + ":: ");
				for(int j=0; j < listVertexFieldNames[i].length; j++){
					System.out.print(listVertexFieldNames[i][j] + "(" + v.getValue(j) + "), ");
				}
				System.out.println();
			}
			s.nextLine();
		}	
		System.out.println();
		System.out.println();
	}

	public void printEdges(){ 		 
		System.out.println("Edges...");
		for(int i=0; i < env.getEdgeTypeCount(); i++){
			ArrayList<Data_Edge> edges = mapEdges.get(i);
			int edgeIndex = 0;
			for(Data_Edge e:edges){
				System.out.print(e.getType() + "_" + edgeIndex + ":: ");
				for(int j=0; j < listVertexFieldNames[i].length; j++){
					System.out.print(listEdgeFieldNames[i][j] + "(" + e.getValue(j) + "), ");
				}
				System.out.println();
				int [] v1 = e.getVertex_In();
				int [] v2 = e.getVertex_Out();
				System.out.println("CONNECTS: {(" + v1[0] + ", " + v1[1] + "), (" + v2[0] + ", " + v2[1] + ")}\n");
			}
			s.nextLine();
		}	
		System.out.println();
		System.out.println();
	}

	private void printMetaData(String type, String[][] fieldNames){
		String separator = "";
		type = type.toUpperCase() + ": ";
		
		for(int i=0; i < fieldNames.length; i++) {
			System.out.print(type);
			separator = "";
			for(int j=0; j < fieldNames[i].length; j++){
				System.out.print(separator + fieldNames[i][j]);
				separator = ", ";
			}
			System.out.println();
		}

		System.out.println();
	}

	public void printMetaData(){
		printMetaData("vertex", listVertexFieldNames);
		printMetaData("edge", listEdgeFieldNames);		
	}
	
	public String nextLine(){
		return s.nextLine();
	}
	
	public void storeVertices(String path, boolean firstPass){
		File vFile = new File(path + "/" + env.getVertexFile());
		if(firstPass){
			File vfFile = new File(path + "/" + env.getVertexFieldsFile());

			System.out.println("Deleting existing files");
			if(vFile.exists()) vFile.delete();
			if(vfFile.exists()) vfFile.delete();
		
			System.out.println("Saving listVertexFieldNames to file");
			try{
				FileWriter fstream = new FileWriter(vfFile);
				BufferedWriter out = new BufferedWriter(fstream);
				String seperator = "";
				for(int i=0; i < listVertexFieldNames.length; i++){
					seperator = "";
					out.write(i + ",");
					for(int j=0; j < listVertexFieldNames[i].length; j++){
						out.write(seperator + listVertexFieldNames[i][j]);
						seperator = ",";
					}
					out.write("\n");
				}
				out.flush();
				out.close();
			} catch(Exception e){
				System.err.println("Error: " + e.getMessage());
			}		
			System.out.println("Saving mapVertices to file");
		}
		
		try{
			FileWriter fstream = new FileWriter(vFile);
			BufferedWriter out = new BufferedWriter(fstream);
			int vertexType = 0;
			for(Integer key:mapVertices.keySet()){
				for(Data_Vertex v:mapVertices.get(key)){
					out.write(vertexType + "");
					int size = v.getSize();
					for(int i=0; i < size; i++){
						out.write("," + v.getValue(i));
					}
					out.write("\n");
				}
				vertexType++;
			}
			out.flush();
			out.close();
		} catch(Exception e){
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void storeEdges(String path, boolean firstPass){		
		File eFile = new File(path + "/" + env.getEdgeFile());
		
		if(firstPass){
			File efieldFile = new File(path + "/" + env.getEdgeFieldsFile());
			
			System.out.println("Deleting existing files");
			if(eFile.exists()) eFile.delete();
			if(efieldFile.exists()) efieldFile.delete();
			
			System.out.println("Saving listEdgeFieldNames to file");
			try{
				FileWriter fstream = new FileWriter(efieldFile);
				BufferedWriter out = new BufferedWriter(fstream);
				String seperator = "";
				for(int i=0; i < listEdgeFieldNames.length; i++){
					seperator = "";
					out.write(i + ",");
					for(int j=0; j < listEdgeFieldNames[i].length; j++){
						out.write(seperator + listEdgeFieldNames[i][j]);
						seperator = ",";
					}
					out.write("\n");
				}
				out.flush();
				out.close();
			} catch(Exception e){
				e.printStackTrace();
			}
			System.out.println("Saving mapEdges to file");
		}
		
		try{
			FileWriter fstream = new FileWriter(eFile, true);
			BufferedWriter out = new BufferedWriter(fstream);
			
			for(Integer key:mapEdges.keySet()){
				for(Data_Edge e:mapEdges.get(key)){
					out.write(key + ",");
					int [] edgeIn = e.getVertex_In();
					int [] edgeOut = e.getVertex_Out();
					out.write(edgeIn[0] + "," + edgeIn[1] + "," + edgeOut[0] + "," + edgeOut[1]);

					int size = e.getSize();
					for(int i=0; i < size; i++){
						out.write("," + e.getValue(i));
					}
					out.write("\n");
				}
			}
			out.flush();
			out.close();
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void print(){
		printMetaData();
		System.out.println();	
		System.out.println("Press enter to continue..");
		nextLine();
		
		printVertices();
		System.out.println("Press enter to continue..");
		nextLine();

		
		printEdges();
		System.out.println("Press enter to continue..");
		nextLine();
	}

}
