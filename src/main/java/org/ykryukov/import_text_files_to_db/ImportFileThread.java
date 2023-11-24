package org.ykryukov.import_text_files_to_db;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;

class ImportFileThread extends Thread {

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
                conn.setAutoCommit(false);
                final ReadFile readFile = new ReadFile(this.filePath);
                final ArrayList<String> fieldNames = readFile.getNextLineAsArrayList(this.delimiter);
                if (fieldNames != null) {
                    final WriteDB writeDB = new WriteDB(conn, this.tableName);
                    if (writeDB.isColumnsNamesCorrect(fieldNames)) {
                        ArrayList<String> fieldValues;
                        int rowsCount = 0;
                        try {
                            while ((fieldValues = readFile.getNextLineAsArrayList(this.delimiter)) != null) {
                                writeDB.writeRow(fieldNames, fieldValues);
                                rowsCount++;
                                if (rowsCount > 999) {
                                    conn.commit();
                                    rowsCount = 0;
                                }
                            }
                            conn.commit();
                        } catch (Exception ex) {
                            conn.rollback();
                            throw ex;
                        }
                    } else {
                        System.out.println("File field name is not correct or field name is not exists!");
                    }
                } else {
                    System.out.println("File is empty!");
                }
            }
            System.out.println("Finished import '" + this.filePath
                    + "' at " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
        } catch (Exception ex) {
            System.out.println(ex);
        }
    }

}