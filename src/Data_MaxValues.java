
public class Data_MaxValues {
	private int maxValue = 999;
	private int vertexTypes;
	private int edgeTypes;
	private int vertexFields;
	private int edgeFields;
	
	Data_MaxValues(int vTypes, int eTypes, int vFields, int eFields){
		this.vertexTypes = vTypes;
		this.edgeTypes = eTypes;
		this.vertexFields = vFields;
		this.edgeFields = eFields;
	}

	public int getMaxValue(){
		return this.maxValue;
	}
	
	public int getVertexTypes(){
		return this.vertexTypes;
	}
	
	public int getEdgeTypes(){
		return this.edgeTypes;
	}
	
	public int getVertexFields(){
		return this.vertexFields;
	}
	
	public int getEdgeFields(){
		return this.edgeFields;
	}
}
