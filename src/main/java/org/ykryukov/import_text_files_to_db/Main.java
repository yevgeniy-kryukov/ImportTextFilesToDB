package org.ykryukov.import_text_files_to_db;

import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;

class Main {
  
  public static void main(String[] args) {
    System.out.println("Started at " + new java.util.Date());
    
    ArrayList<Thread> listThreads = new ArrayList<Thread>();
    
    try {
      final ReadFile listFiles = new ReadFile("list_files.txt");
      int i = 0;
      ArrayList<String> line;
      while ((line = listFiles.getNextLineAsArrayList(";")) != null) {
        i++;
        Thread t = new ImportFileThread("ImportFileThread" + i, line.get(0), line.get(1), line.get(2));
        t.start();
        listThreads.add(t);
        System.out.println("Started import '" + line.get(0) + "'");
      }
    } catch(Exception ex) {
      System.out.println(ex);
    } 
    
    for(Thread item : listThreads) {
      try {
        item.join();        
      } catch(InterruptedException e){
        System.out.printf("%s has been interrupted", item.getName());
      }
    }
    
    System.out.println("Finished at " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
    
  }
  
 }