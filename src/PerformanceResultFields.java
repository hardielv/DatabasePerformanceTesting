
public enum PerformanceResultFields {
		DATE(0, "Date"), 
		HEAPSIZE(1, "HeapSize"), 
		DB_VENDOR(2, "DB Vendor"), 
		DB_NAME(3, "DB Name"), 
		QUERY(4, "Query"), 
		ITERATIONS(5, "Iterations"), 
		DEPTH(6, "Depth"), 
		VERTEX_COUNT(7, "Vertices"), 
		EDGE_COUNT(8, "Edges"), 
		AVG_RECORDS(9, "AvgRecs"), 
		AVG_TIME_MS(10, "Avg Time(ms)"), 
		AVG_TIME_MIN(11, "Avg Time(min)"),
		AVG_TIME_SEC(12, "Avg Time(sec)"),
		AVG_CPU_TIME_MS(13, "Avg CPU Time(ms)"), 
		AVG_CPU_TIME_MIN(14, "Avg CPU Time(min)"),
		AVG_CPU_TIME_SEC(15, "Avg CPU Time(sec)");

		public final static int fieldCount = 16;
		private String title;
		private int index;
		
		PerformanceResultFields(int i, String title){
			this.index = i;
			this.title = title;
		}
		public String toString(){
			return this.title;
		}
		public int getIndex(){
			return this.index;
		}
}
