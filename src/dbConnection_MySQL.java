import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;


public class dbConnection_MySQL implements DatabaseConnection_Interface {
	// DATABASE TYPE SPECIFIC
	String DB_TYPE = "mysql";
	String DB_PATH;
	String DB_NAME;

	final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   	final String DB_URL = "jdbc:mysql://localhost:3307/";
   	Connection db;
   	Statement stmt;
	//  Database credentials
	static final String USER = "lexgrid";
	static final String PASS = "lexgrid";
	
	DatabaseEnvironment env;
	Long [] randomRID_list;
   	
	private HashSet<Object> GRAPH1, GRAPH2;
	

	public dbConnection_MySQL(DatabaseEnvironment e, String size){
		env = e;
		DB_NAME = env.DB_NAME_PREFIX + size;
		DB_PATH = env.DB_PATH;
		GRAPH1 = new HashSet<Object>();
		GRAPH2 = new HashSet<Object>();
		open();
}
	
	
	@Override
	public void open() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			db = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = db.createStatement();
			stmt.executeUpdate("USE " + DB_NAME);
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (ClassNotFoundException ce) {
			ce.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
			stmt.close();
			db.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public String getDatabaseName() {
		return DB_NAME;
	}

	@Override
	public Object getRandomRID(int i) {
		return randomRID_list[i];
	}

	@Override
	public void setRandomRID(int i, Object value) {
		randomRID_list[i] = (Long) value;
	}

	@Override
	public void collectRandomRIDs(int iterations) {
		// Add #iterations plus one for the set operations to have two subgraphs per iteration
		int numRIDs = iterations + 1;
		randomRID_list = new Long[numRIDs];
		String randomTableName, primaryKey, sql;
		ResultSet idRange;
		long randomPosition;
		long minID, maxID;
		// Collect #iterations of random RID's
		for(int i=0; i < numRIDs; i++){
			randomTableName = env.VERTEX_PREFIX + (int) (Math.random() * env.NUM_VERTEX_TYPE);
			primaryKey = randomTableName + "_ID";
			try {
				stmt = db.createStatement();
				sql = "Select min(" + primaryKey + ") as minID, max(" + primaryKey + ") as maxID from " + randomTableName;
				idRange = stmt.executeQuery(sql);
								
				if(idRange.next()){
					minID = idRange.getInt("minID");
					maxID = idRange.getInt("maxID");
					
					randomPosition = (long) (Math.random() * (maxID)) + minID;
					randomRID_list[i] = randomPosition;
				}
				else{
					System.out.println("No min/max values returned");
				}
				
				idRange.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public HashSet<Object> traverseJava(Object root, int level, int depth, int graphID){
		String sql;
		Long id;
		if (graphID == 1) {
			GRAPH1.add(root);
		} else {
			GRAPH2.add(root);
		}
		if (level <= depth) {
			sql = "Select edgeOUT from tempEdges where edgeIN = " + root;
			Statement stmt2;
			try {
				stmt2 = db.createStatement();
				ResultSet resultSet = stmt2.executeQuery(sql);
				while (resultSet.next()) {
					id = resultSet.getLong("edgeOUT");
					traverseJava(id, level + 1, depth, graphID);
				}
				resultSet.close();
				stmt2.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(graphID ==1) return GRAPH1;
		return GRAPH2;
	}

	@Override
	public HashSet<Object> getGraph(int i) {
		if(i == 1) return GRAPH1;
		return GRAPH2;
	}

	
	@Override
	public void printRecords(List<Object> recList) {
		// TODO Auto-generated method stub
		
	}

	// ------------------------------------------------------------
	
	private String createSQL_Traverse(long id, int depth){
		String sql = "select ";
		
		String sqlFields = "", sqlTables = "", sqlWhere = "", sqlOrder = "";
		String comma = "";
		for(int i=1; i <= depth; i++){
			sqlFields += comma + "L" + i + ".edgeOUT as L" + i + "_OUT, L" + i + ".edgeIN as L" + i + "_IN";
			sqlTables += comma + "tempEdges as L" + i;
			sqlOrder += comma + "L" + i + ".edgeOUT";
			if(i>1){
				sqlWhere += "L" + (i-1) + ".edgeIN = L" + i + ".edgeOUT and ";
			}
			comma = ", ";
		}
		
		sql = "Select " + sqlFields + " from " + sqlTables + " where " + sqlWhere + " L1.edgeOUT = " + id;
		sql += " order by " + sqlOrder;
		return sql;
	}
}
