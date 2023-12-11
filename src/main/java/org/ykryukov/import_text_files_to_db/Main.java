package org.ykryukov.import_text_files_to_db;

import java.util.*;
import java.text.SimpleDateFormat;

class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("Started at " + new java.util.Date());

        List<ImportFileThread> listThreads = new ArrayList<ImportFileThread>();

        final Properties properties = ResourceFileUtil.getFileProperties("app.properties");
        final ReadFile listFiles = new ReadFile(properties.getProperty("configFilePath"));
        int n = 0;
        List<String> line;
        while ((line = listFiles.getNextLineAsArrayList(";")) != null) {
            n++;
            ImportFileThread t = new ImportFileThread("ImportFileThread" + n, line.get(0), line.get(1), line.get(2));
            listThreads.add(t);
        }

        final int countThreads = 3;
        List<ImportFileThread> listThreadsStarted = new ArrayList<>();
        ImportFileThread thread;
        Iterator<ImportFileThread> listThreadsIt = listThreads.iterator();
        for (int i = 0; i < Math.ceil((float) listThreads.size() / countThreads); i++) {
            for (int j = 0; j < countThreads; j++) {
                if (listThreadsIt.hasNext()) {
                    thread = listThreadsIt.next();
                    thread.start();
                    listThreadsStarted.add(thread);
                    System.out.println("Started import '" + thread.getFilePath() + "'");
                }
            }
            for (ImportFileThread threadStarted : listThreadsStarted) {
                threadStarted.join();
            }
            listThreadsStarted.clear();
        }

        ConnDB.closeAllConnections();

        System.out.println("Finished at " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));

    }

}