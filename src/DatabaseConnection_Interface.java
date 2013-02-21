import java.util.HashSet;
import java.util.List;


interface DatabaseConnection_Interface {

	public void open();
	public void close();
	public String getDatabaseName();
	
	public Object getRandomRID(int i);
	public void setRandomRID(int i, Object value);
	public void collectRandomRIDs(int iterations);
	
//	public HashSet<Object> traverseJava(Object root, int depth) throws OutOfMemoryError;
	public HashSet<Object> traverseJava(Object root, int level, int depth, int graphID);	
	public void printRecords(List<Object> recList);

	public HashSet<Object> getGraph(int i);
}
