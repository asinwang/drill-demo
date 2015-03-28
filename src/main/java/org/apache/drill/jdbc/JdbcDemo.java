package org.apache.drill.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;

/**
 * Unit test for simple JdbcDemo.
 */
public class JdbcDemo {
	private static CachingConnectionFactory factory;

	static {
		factory = new SingleConnectionCachingFactory(new ConnectionFactory() {
			@Override
			public Connection createConnection(ConnectionInfo info) throws Exception {
				Class.forName("org.apache.drill.jdbc.Driver");
				return DriverManager.getConnection(info.getUrl(), info.getParamsAsProperties());
			}
		});
	}
	
	private static final String URL = "jdbc:drill:zk=xuansheng-pc";
	
	public static void main(String[] args) throws Exception {
		JdbcDemo jdbcDemo = new JdbcDemo();
		String sql = "SELECT * FROM cp.`employee.json` LIMIT 20";
		Connection conn = connect(URL);
		jdbcDemo.query(conn,sql);
	}
	
	public void query(Connection conn,String sql) throws Exception {
		boolean success = false;
		try {
			for (int x = 0; x < 1; x++) {
				Stopwatch watch = new Stopwatch().start();
				Statement s = conn.createStatement();
				ResultSet r = s.executeQuery(sql);
				// System.out.println(String.format("QueryId: %s",
				// ((DrillResultSet) r).getQueryId()));
				boolean first = true;
				while (r.next()) {
					ResultSetMetaData md = r.getMetaData();
					if (first == true) {
						for (int i = 1; i <= md.getColumnCount(); i++) {
							System.out.print(md.getColumnName(i));
							System.out.print('\t');
						}
						System.out.println();
						first = false;
					}

					for (int i = 1; i <= md.getColumnCount(); i++) {
						System.out.print(r.getObject(i));
						System.out.print('\t');
					}
					System.out.println();
				}

				System.out.println(String.format("Query completed in %d millis.", watch.elapsed(TimeUnit.MILLISECONDS)));
			}

			System.out.println("\n\n\n");
			success = true;
		} finally {
			if (!success) {
				Thread.sleep(2000);
			}
		}
	}

	/**
	 * Creates a {@link java.sql.Connection connection} using default
	 * parameters.
	 * 
	 * @param url
	 *            connection URL
	 * @throws Exception
	 *             if connection fails
	 */
	protected static Connection connect(String url) throws Exception {
		return connect(url, getDefaultProperties());
	}

	public static Properties getDefaultProperties() {
		final Properties properties = new Properties();
		properties.setProperty("drill.exec.http.enabled", "false");
		return properties;
	}

	/**
	 * Creates a {@link java.sql.Connection connection} using the given
	 * parameters.
	 * 
	 * @param url
	 *            connection URL
	 * @param info
	 *            connection info
	 * @throws Exception
	 *             if connection fails
	 */
	protected static Connection connect(String url, Properties info) throws Exception {
		final Connection conn = factory.createConnection(new ConnectionInfo(url, info));
		changeSchemaIfSupplied(conn, info);
		return conn;
	}

	/**
	 * Changes schema of the given connection if the field "schema" is present
	 * in {@link java.util.Properties info}. Does nothing otherwise.
	 */
	protected static void changeSchemaIfSupplied(Connection conn, Properties info) {
		final String schema = info.getProperty("schema", null);
		if (!Strings.isNullOrEmpty(schema)) {
			changeSchema(conn, schema);
		}
	}

	protected static void changeSchema(Connection conn, String schema) {
		final String query = String.format("use %s", schema);
		try {
			Statement s = conn.createStatement();
			ResultSet r = s.executeQuery(query);
		} catch (SQLException e) {
			throw new RuntimeException("unable to change schema", e);
		}
	}

}
