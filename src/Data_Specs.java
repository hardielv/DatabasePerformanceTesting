
public class Data_Specs {
	private Data_Size size;
	private Data_MaxValues maxValues;
	
	public Data_Specs(Data_Size s, Data_MaxValues values){
		size = s;
		maxValues = values;
	}

	public String getSize(){ return this.size.toString(); }
	public int getVertexCount(){ return this.size.getVertexCount(); }	
	public int getEdgeCount(){ return this.size.getEdgeCount(); }
	
	public int getMaxValue() { return maxValues.getMaxValue(); }	
	public int getMaxVertexTypes(){ return maxValues.getVertexTypes(); }
	public int getMaxEdgeTypes(){ return maxValues.getEdgeTypes(); }
	public int getMaxVertexFields() { return maxValues.getVertexFields(); }
	public int getMaxEdgeFields() { return maxValues.getEdgeFields(); }
}