import java.util.ArrayList;


public class PerformanceResults {
	
//	final static String DELIMITER = ",";
	final static String DELIMITER = "\t";
	
//	Fields []  fields = Fields.values();
	String [] values;
	
	public PerformanceResults(){
		values = new String[PerformanceResultFields.fieldCount];
	}
	
	public void saveField(PerformanceResultFields field, String value){
		values[field.getIndex()] = value;
	}
	
	public String toString(){
		String line = "";
		int end = values.length - 1;
		for(int i=0; i < end; i++){
			line += values[i] + DELIMITER;
		}
		
		line += values[end];
		return line;
	}
	
	public String getHeaders(){
		String line = "";
		PerformanceResultFields [] fields = PerformanceResultFields.values();
		int end = fields.length - 1;
		for(int i=0; i < end; i++){
			line += fields[i].toString() + DELIMITER;
		}
		
		line += fields[end].toString();
		return line;
	}
}

