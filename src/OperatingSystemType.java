
public enum OperatingSystemType  {
	
	WINDOWS("windows"), 
	LINUX("linux");
	
	private String name;

	OperatingSystemType(String n){
		this.name= n;
	}
	
	public String getName(){
		return this.name;
	}
}
