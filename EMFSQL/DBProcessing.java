package EMFSQL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
public class DBProcessing {
	private Connection conn;
	
	public DBProcessing(){
		this.conn = DBConnection.getDBInstance();
	}
	
	// Execute Query
	public ResultSet QueryExecute(String query){
		ResultSet rs = null;
		Statement st=null;
		try {
			st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			rs = st.executeQuery(query);
		}catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	// Query Execution To Get DataType
	public String GetDataType(String columnname){
		Statement st = null;
		String result = null;
		try {
			st = conn.createStatement();
			
			String query =  "select data_type from information_schema.columns where table_name='sales' and column_name = '"+columnname+"'";
			ResultSet rs = st.executeQuery(query);
			while(rs.next()){
				result = rs.getString(1);
				break;
			}
			rs.close();
			return result;
		}
		catch (SQLException e) {
			System.out.println("Fail DataType Acquiring!");
			e.printStackTrace();
			return null;
		}
		finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					System.out.println("Fail Close Statement");
					e.printStackTrace();
				}
			}
			
		}
	}
}
