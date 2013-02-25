
public enum Database_QueryType {
	TRAVERSE("traverse"), 
	UNION("union"), 
	INTERSECTION("intersection"), 
	DIFFERENCE("difference"), 
	SYMMETRIC_DIFFERENCE("symmetric difference");
	
	private String name;
	
	Database_QueryType(String n){
		this.name= n;
	}
	
	public String getName(){
		return this.name;
	}
}

