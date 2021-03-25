package org.ykryukov.import_text_files_to_db;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;

class ReadFile {
  final private FileInputStream fileInputStream;
  final private InputStreamReader inputStreamReader;
  final private BufferedReader bufferedReader;
  
  ReadFile(final String filePath) throws IOException {
    fileInputStream = new FileInputStream(filePath);
    inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
    bufferedReader = new BufferedReader(inputStreamReader);
  }
  
  private ArrayList<String> parseLineToArrayList(final String str, final String delimiter) {
    final ArrayList<String> list1 = new ArrayList<String>();
    for(String item: str.split("\\" + delimiter)) {
      list1.add(item);
    }
    return list1;
  }
  
  private void closeStreams() throws IOException {
    bufferedReader.close();
    inputStreamReader.close();
    fileInputStream.close();
  }
    
  ArrayList<String> getNextLineAsArrayList(final String delimiter) throws IOException {
    try {
      final String line = bufferedReader.readLine();
      if (line != null) {
        return parseLineToArrayList(line, delimiter);
      } else {
        closeStreams();
        return null;
      }
    } catch (IOException ex) {
      closeStreams();
      throw ex;
    }
  }
}