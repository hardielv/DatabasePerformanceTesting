
public enum Database_Vendor {
	ORIENTDB("orientDB"), 
	MYSQL("mySQL"),
	NEO4J("neo4j");
	
	private String name;
	
	Database_Vendor(String n){
		this.name = n;
	}
	
	public String getName(){
		return this.name;
	}
	
}
