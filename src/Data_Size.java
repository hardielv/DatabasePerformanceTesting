
public enum Data_Size {
	SMALL("small", 100, 500), 
	MEDIUM("medium", 1000, 5000), 
	LARGE("large", 10000, 50000), 
	HUGE("huge", 1000000, 5000000),
	GIGANTIC("gigantic", 1500000, 8000000);

	private String name;
	private int vertexCount, edgeCount;
	
	Data_Size(String n, int v, int e){
		this.name= n;
		this.vertexCount = v;
		this.edgeCount = e;
	}
	
	public String toString(){
		return this.name;
	}
	
	public int getVertexCount(){
		return this.vertexCount;
	}
	
	public int getEdgeCount(){
		return this.edgeCount;
	}

}
