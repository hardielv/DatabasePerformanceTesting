import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;


interface Database_Connection_Interface {

	public boolean open();
	public boolean delete();
	public void close();
	public void create();
	
	
	public void setLargeInsert();
	public void unsetLargeInsert();
	public String getDatabaseName();
	public void createVertexTable(String table, String indexField, ArrayList<String> fieldnames);
	public void createEdgeTable(String table, String indexField, ArrayList<String> fieldnames);
	public Object storeVertex(String table, String idField, Long id, ArrayList<String> fieldnames, Scanner lineScan);
	public void storeEdges(File eFile, 
			HashMap<Integer, ArrayList<String>> mapEdgeFieldNames, 
			HashMap<Integer, ArrayList<Object>> mapVertexRIDs);
	
	public Object getRandomRID(int i);
	public void setRandomRID(int i, Object value);
	public void collectRandomRIDs(int iterations);
	
//	public HashSet<Object> traverseJava(Object root, int depth) throws OutOfMemoryError;
	public HashSet<Object> traverseJava(Object root, int level, int depth, int graphID);	
	public void printRecords(List<Object> recList);

	public HashSet<Object> getGraph(int i);
	public void createPrimaryKeys();
	public void createIndices();
	public void clearEdges();
	
	
}
