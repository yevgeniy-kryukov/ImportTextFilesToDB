package org.ykryukov.import_text_files_to_db;

import java.io.*;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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
        return new ArrayList<String>(Arrays.asList(str.split("\\" + delimiter)));
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