package net.lannysport.TeradataCLI;

import java.sql.*;
import java.io.PrintStream;
import java.io.OutputStream;

public class App {
    public static void main( String[] args ) {
		try {
			Class.forName("com.teradata.jdbc.TeraDriver");
		} catch (ClassNotFoundException e) {
			System.err.println(
				"Couldn't load Teradata JDBC driver. Terminating.");
			System.exit(1);
		}

		String dburl = "jdbc:teradata://C2T.WELLSFARGO.COM/DATABASE=DVU_111,LOGMECH=LDAP";
		try {
			Connection conn = DriverManager.getConnection(dburl, "", "");
			PreparedStatement stmt = conn.prepareStatement("SELECT 1");
			stmt.execute();
			ResultSet rs = stmt.getResultSet();
			outputQueryResult(rs, System.out);
			stmt.close();
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			return;
		}
		
        System.out.println( "Hello World!" );
    }
	private static void outputQueryResult(ResultSet result, OutputStream outputStream)
			throws SQLException {
		final PrintStream outFile = new PrintStream(outputStream);
		final String delim = ",";
		boolean isHeaderPending = true;
		if (result != null) {
			while (result.next()) {
				int numColumns = result.getMetaData().getColumnCount();
				StringBuilder dataString = new StringBuilder();
				if (isHeaderPending) {
					StringBuilder headerString = new StringBuilder();
					for (int j = 1; j <= numColumns; j++) {
						String colName = result.getMetaData().getColumnName(j);
						if (j > 1) {
							headerString.append(delim).append(colName);
						} else {
							headerString.append(colName);
						}
					}
					isHeaderPending = false;
					outFile.println(headerString.toString());
				}
				for (int j = 1; j <= numColumns; j++) {
					String colVal = result.getString(j);
					if (colVal == null) {
						colVal = "\"null\"";
					}

					if (j > 1) {
						dataString.append(delim).append(colVal);
					} else {
						dataString.append(colVal);
					}
				}

				outFile.println(dataString.toString());
			}
		}
		outFile.close();
	}
	
}
