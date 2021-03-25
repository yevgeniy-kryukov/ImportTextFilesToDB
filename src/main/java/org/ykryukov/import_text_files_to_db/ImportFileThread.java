package org.ykryukov.import_text_files_to_db;

import java.sql.Connection;
import java.util.ArrayList;

public class ImportFileThread extends Thread {
  
  final private String filePath;
  
  final private String tableName;
  
  final private String delimiter;
  
  ImportFileThread(String threadName, String filePath, String tableName, String delimiter) {
    super(threadName);
    this.filePath = filePath; 
    this.tableName = tableName;
    this.delimiter = delimiter;
  }
  
  public void run() {
    try {
      try (final Connection conn = ConnDB.getConnection()) {
        final ReadFile readFile = new ReadFile(this.filePath);
        final ArrayList<String> fieldNames = readFile.getNextLineAsArrayList(this.delimiter);
        if (fieldNames != null) {
          final WriteDB writeDB = new WriteDB(conn, this.tableName);
          if (writeDB.isColumnsNamesCorrect(fieldNames)) {
            ArrayList<String> fieldValues;
            while ((fieldValues = readFile.getNextLineAsArrayList(this.delimiter)) != null) {
              writeDB.writeRow(fieldNames, fieldValues);
            }
          } else {
            System.out.println("File field name is not correct or field name is not exists!");
          }
        } else {
          System.out.println("File is empty!");
        }
      }
      System.out.println("Finished import '" + this.filePath + "'");
    } catch(Exception ex) {
      System.out.println(ex);
    }  
  }
  
}