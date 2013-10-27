package com.tech.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Sample created for IBM iSeries DB2 database. This will test DB connection by
 * executing query on a table and displaying meta data about that table.
 * 
 * @author salil.walavalkar
 * 
 */
public class JDBC_Conn_Test {

	static final String URL = "jdbc:as400://[IP Address Or Hostname]/[SCHEMA]";

	static final String USERNAME = "<Enter username here>";
	static final String PASSWORD = "<Enter password here>";

	public static void main(String[] args) {
		Connection con;
		Statement stmt;
		ResultSet rs;

		try {
			// Load the driver : For DB2 only
			Class.forName("com.ibm.as400.access.AS400JDBCDriver");

			// Create the connection.
			con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
			
			// Commit changes manually
			con.setAutoCommit(false);

			// Create the Statement
			stmt = con.createStatement();

			// Execute a query and generate a ResultSet instance
			rs = stmt.executeQuery("select * from <Enter table name here>");

			int cnt = 0;
			while (rs.next()) {
				System.out.println("Data:" + rs.getString(1));
				cnt++;
			}
			System.out.println("Rows: " + cnt);

			writeMetaData(rs);

			// Close the ResultSet
			rs.close();

			// Close the Statement
			stmt.close();

			// Close the connection
			con.close();
		}

		catch (ClassNotFoundException e) {
			System.err.println("Could not load JDBC driver");
			System.out.println("Exception: " + e);
			e.printStackTrace();
		}

		catch (SQLException ex) {
			System.err.println("SQLException information");
			while (ex != null) {
				System.err.println("Error msg: " + ex.getMessage());
				System.err.println("SQLSTATE: " + ex.getSQLState());
				System.err.println("Error code: " + ex.getErrorCode());
				ex.printStackTrace();
				ex = ex.getNextException(); // For drivers that support chained
											// exceptions
			}
		}
	}

	/**
	 * Write Meta data about table
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	private static void writeMetaData(ResultSet resultSet) throws SQLException {
		System.out.println("The columns in the table are: ");

		System.out.println("Table: " + resultSet.getMetaData().getTableName(1));
		for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
			System.out.println("Column " + i + " "
					+ resultSet.getMetaData().getColumnName(i));
		}
	}
}