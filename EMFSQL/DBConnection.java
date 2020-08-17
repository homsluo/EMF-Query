package EMFSQL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
public class DBConnection {
	private static Connection db;
	private static final String user = "postgres";
	private static final String password = "lyq36719";
	private static final String url = "jdbc:postgresql://localhost:5432/postgres";
	
	// Load Driver
	public static void loadDBDriver() {
		try {
			Class.forName("org.postgresql.Driver");
			System.out.println("Success loading Driver!\n");
		} catch (Exception exception) {
			System.out.println("Fail loading Driver!");
			exception.printStackTrace();
		}
	}
	
	// Try Connecting DB
	public static Connection getDBInstance() {
		if (db == null)
			try {
				loadDBDriver();
				db = DriverManager.getConnection(url, user, password);
				System.out.println("Success Connect Database!\n");
			} catch (SQLException e) {
				System.out.println("Fail Connecting!");
				e.printStackTrace();
			}
		return db;
	}
}
