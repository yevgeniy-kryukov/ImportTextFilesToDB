package org.ykryukov.import_text_files_to_db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.io.InputStream;
import java.util.Properties;

class ConnDB {
    private ConnDB() {
    }

    static Connection getConnection() throws Exception {
        Class.forName("org.postgresql.Driver").getDeclaredConstructor().newInstance();
        final Properties props = ResourceFileUtil.getFileProperties("app.properties");
        final String url = props.getProperty("url");
        final String username = props.getProperty("username");
        final String password = props.getProperty("password");

        return DriverManager.getConnection(url, username, password);
    }
}