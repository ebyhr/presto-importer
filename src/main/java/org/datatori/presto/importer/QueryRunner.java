/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datatori.presto.importer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class QueryRunner
{
    private final Connection connection;

    public QueryRunner(String server, String user, Optional<String> password)
            throws ClassNotFoundException, SQLException
    {
        Class.forName("io.prestosql.jdbc.PrestoDriver");

        String url = "jdbc:presto://" + server;
        Properties properties = new Properties();
        properties.setProperty("user", user);
        password.ifPresent(pass -> properties.setProperty("password", pass));
        this.connection = DriverManager.getConnection(url, properties);
    }

    public void dropCreateTable(CatalogSchemaTableName table)
            throws SQLException
    {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW CREATE TABLE " + table)) {
            StringBuilder createTable = new StringBuilder();
            while (resultSet.next()) {
                createTable.append(resultSet.getString(1));
            }
            if (createTable.length() == 0) {
                throw new RuntimeException(format("Table %s does not exist", table));
            }
            System.out.println("Drop table " + table);
            statement.execute("DROP TABLE IF EXISTS " + table);

            System.out.println("Create table " + table);
            statement.execute(createTable.toString());
        }
    }

    public Map<String, String> getColumns(CatalogSchemaTableName table)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        ResultSet resultSet = metadata.getColumns(table.getCatalog(), table.getSchema(), table.getTable(), null);
        Map<String, String> columnTypes = new LinkedHashMap<>();
        while (resultSet.next()) {
            String name = resultSet.getString("COLUMN_NAME");
            String type = resultSet.getString("TYPE_NAME");
            columnTypes.put(name, type);
        }
        if (columnTypes.isEmpty()) {
            throw new RuntimeException(format("Table %s does not exist", table));
        }
        return columnTypes;
    }

    public void insertRows(CatalogSchemaTableName table, Map<String, String> columns, List<String[]> rows)
            throws SQLException
    {
        checkArgument(!columns.isEmpty(), "At least one column should exist");

        String placeholders = columns.keySet().stream().map(entry -> "?").collect(Collectors.joining(", "));
        String values = rows.stream().map(row -> format("(%s)", placeholders)).collect(Collectors.joining(", "));

        try (PreparedStatement insertStatement = connection.prepareStatement(format("INSERT INTO %s VALUES %s", table, values))) {
            int entire = 1;
            for (String[] row : rows) {
                int position = 1;
                for (Map.Entry<String, String> column : columns.entrySet()) {
                    String type = column.getValue().split("\\(")[0];
                    String value = row[position++ - 1];

                    switch (type) {
                        case "boolean":
                            insertStatement.setBoolean(entire++, Boolean.valueOf(value));
                            break;
                        case "tinyint":
                            insertStatement.setByte(entire++, Byte.valueOf(value));
                            break;
                        case "smallint":
                            insertStatement.setShort(entire++, Short.valueOf(value));
                            break;
                        case "integer":
                        case "bigint":
                            insertStatement.setInt(entire++, Integer.parseInt(value));
                            break;
                        case "real":
                            insertStatement.setFloat(entire++, Float.parseFloat(value));
                            break;
                        case "double":
                            insertStatement.setDouble(entire++, Double.parseDouble(value));
                            break;
                        case "decimal":
                            insertStatement.setBigDecimal(entire++, new BigDecimal(value));
                            break;
                        case "varchar":
                        case "char":
                            insertStatement.setString(entire++, value);
                            break;
                        case "date":
                            insertStatement.setDate(entire++, Date.valueOf(value));
                            break;
                        case "varbinary":
                        case "json":
                        case "time":
                        case "time with timezone":
                        case "timestamp":
                        case "timestamp with time zone":
                        case "interval year to month":
                        case "interval year to second":
                        case "array":
                        case "map":
                        case "row":
                        case "ipaddress":
                        case "HyperLogLog":
                        case "P4HyperLogLog":
                        default:
                            throw new RuntimeException("Unsupported column type: " + type);
                    }
                }
            }
            System.out.println(format("Insert %s rows", rows.size()));
            insertStatement.execute();
        }
    }
}
