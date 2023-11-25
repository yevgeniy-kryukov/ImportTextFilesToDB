package org.ykryukov.import_text_files_to_db;

import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Properties;

class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("Started at " + new java.util.Date());

        ArrayList<Thread> listThreads = new ArrayList<Thread>();

        final Properties properties = ResourceFileUtil.getFileProperties("app.properties");
        final ReadFile listFiles = new ReadFile(properties.getProperty("configFilePath"));
        int i = 0;
        ArrayList<String> line;
        while ((line = listFiles.getNextLineAsArrayList(";")) != null) {
            i++;
            Thread t = new ImportFileThread("ImportFileThread" + i, line.get(0), line.get(1), line.get(2));
            t.start();
            listThreads.add(t);
            System.out.println("Started import '" + line.get(0) + "'");
        }

        for (Thread item : listThreads) {
            item.join();
        }

        System.out.println("Finished at " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));

    }

}