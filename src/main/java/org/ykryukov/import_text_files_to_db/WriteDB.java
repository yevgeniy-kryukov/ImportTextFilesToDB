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
  
  WriteDB(final Connection connenction, final String tableName) throws SQLException {
    conn = connenction;
    tbName = tableName;
    final String query = "SELECT * FROM " + tbName + " LIMIT 1";
    try(final Statement statement = conn.createStatement();
        final ResultSet resultSet = statement.executeQuery(query)) {
      final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      columnsNamesList = new ArrayList<String>();
      columnsTypes = new HashMap<String, String>();
      for(int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        columnsTypes.put(resultSetMetaData.getColumnName(i), resultSetMetaData.getColumnTypeName(i));
        columnsNamesList.add(resultSetMetaData.getColumnName(i));
      }
    }
  }

  boolean isColumnsNamesCorrect(ArrayList<String> columnsNamesListFile) {      
    boolean found;
    for (String itemColumnNameFile: columnsNamesListFile) {
      found = false;
      for (String itemColumnName: columnsNamesList) {
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
    return columnsTypes.get(columnName);
  }
  
  private void setPrepareStatementValues(PreparedStatement preparedStatement, final ArrayList<String> fieldNames, final ArrayList<String> fieldValues) throws SQLException {
    int i = 0;
    String typeName = "";
    for (String item : fieldValues) {
      i++;
      typeName = getColumnTypeByName(fieldNames.get(i-1));
      if (typeName == "int8") {
        preparedStatement.setLong(i, Long.parseLong(item));
      } else if (typeName == "varchar") {
        preparedStatement.setString(i, item);
      } else if (typeName == "text") {
        preparedStatement.setString(i, item);
      }
    } 
  }
  
  boolean writeRow(final ArrayList<String> fieldNames, final ArrayList<String> fieldValues) throws SQLException {
    String sqlFieldNames = "";
    String sqlFieldMaskValues = "";
    String sqlDelimeter = "";
    for (String item : fieldNames) {
      sqlFieldNames = sqlFieldNames + sqlDelimeter + item;
      sqlFieldMaskValues = sqlFieldMaskValues + sqlDelimeter + "?";
      sqlDelimeter =  ",";
    }
    final String sqlText = "INSERT INTO " + tbName + " (" + sqlFieldNames + ") VALUES (" + sqlFieldMaskValues + ")";
    try(final PreparedStatement preparedStatement = conn.prepareStatement(sqlText)) {
      setPrepareStatementValues(preparedStatement, fieldNames, fieldValues);
      final int rowsCount = preparedStatement.executeUpdate();
    }
    return true;
  }
}