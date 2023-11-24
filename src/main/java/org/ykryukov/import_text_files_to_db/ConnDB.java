package org.ykryukov.import_text_files_to_db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

class ConnDB {
    private ConnDB() {
    }

    static Connection getConnection() throws Exception {
        Class.forName("org.postgresql.Driver").getDeclaredConstructor().newInstance();
        final Properties props = ResourceFileUtil.getFileProperties("app.properties");
        return DriverManager.getConnection(props.getProperty("url"),
                props.getProperty("username"),
                props.getProperty("password"));
    }
}