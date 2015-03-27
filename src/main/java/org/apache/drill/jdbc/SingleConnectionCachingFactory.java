/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.google.common.base.Strings;

/**
 * A connection factory that creates and caches a single connection instance.
 *
 * Not thread safe.
 */
public class SingleConnectionCachingFactory implements CachingConnectionFactory {

  private final ConnectionFactory delegate;
  private Connection connection;

  public SingleConnectionCachingFactory(ConnectionFactory delegate) {
    this.delegate = delegate;
  }

  @Override
  public Connection createConnection(ConnectionInfo info) throws Exception {
    if (connection == null) {
      connection = delegate.createConnection(info);
    } else {
      changeSchemaIfSupplied(connection, info.getParamsAsProperties());
    }
    return new NonClosableConnection(connection);
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
  
  @Override
  public void close() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }
}
