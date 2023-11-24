package org.ykryukov.import_text_files_to_db;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.ArrayList;

class WriteDB {
    final private Connection conn;
    final private String tbName;
    final private ArrayList<String> columnsNamesList;
    final private HashMap<String, String> columnsTypes;

    WriteDB(final Connection connection, final String tableName) throws SQLException {
        this.conn = connection;
        this.tbName = tableName;
        final String query = "SELECT * FROM " + this.tbName + " LIMIT 1";
        try (final Statement statement = this.conn.createStatement();
             final ResultSet resultSet = statement.executeQuery(query)) {
            final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            this.columnsNamesList = new ArrayList<String>();
            this.columnsTypes = new HashMap<String, String>();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                this.columnsTypes.put(resultSetMetaData.getColumnName(i), resultSetMetaData.getColumnTypeName(i));
                this.columnsNamesList.add(resultSetMetaData.getColumnName(i));
            }
        }
    }

    boolean isColumnsNamesCorrect(ArrayList<String> columnsNamesListFile) {
        boolean found;
        for (String itemColumnNameFile : columnsNamesListFile) {
            found = false;
            for (String itemColumnName : this.columnsNamesList) {
                if (itemColumnNameFile.equals(itemColumnName)) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    }

    private String getColumnTypeByName(final String columnName) {
        return this.columnsTypes.get(columnName);
    }

    private void setPrepareStatementValues(PreparedStatement preparedStatement,
                                           final ArrayList<String> fieldNames,
                                           final ArrayList<String> fieldValues) throws SQLException {
        int i = 0;
        for (String item : fieldValues) {
            i++;
            switch (getColumnTypeByName(fieldNames.get(i - 1))) {
                case "int8":
                    preparedStatement.setLong(i, Long.parseLong(item));
                    break;
                case "varchar":
                case "text":
                    preparedStatement.setString(i, item);
                    break;
            }
        }
    }

    void writeRow(final ArrayList<String> fieldNames, final ArrayList<String> fieldValues) throws SQLException {
        StringBuilder sqlFieldNames = new StringBuilder();
        StringBuilder sqlFieldMaskValues = new StringBuilder();
        String sqlDelimeter = "";
        for (String item : fieldNames) {
            sqlFieldNames.append(sqlDelimeter).append(item);
            sqlFieldMaskValues.append(sqlDelimeter).append("?");
            sqlDelimeter = ",";
        }
        final String sqlText = "INSERT INTO " + this.tbName + " (" + sqlFieldNames + ") VALUES (" + sqlFieldMaskValues + ")";
        try (final PreparedStatement preparedStatement = this.conn.prepareStatement(sqlText)) {
            setPrepareStatementValues(preparedStatement, fieldNames, fieldValues);
            final int rowsCount = preparedStatement.executeUpdate();
        }
    }
}