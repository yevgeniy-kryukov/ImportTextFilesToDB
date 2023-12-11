package org.ykryukov.import_text_files_to_db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

class ConnDB {
    private static Map<ConnDB, Thread> instances;
    final private Connection connection;

    private ConnDB() throws Exception {
        Class.forName("org.postgresql.Driver").getDeclaredConstructor().newInstance();
        final Properties props = ResourceFileUtil.getFileProperties("app.properties");
        this.connection = DriverManager.getConnection(props.getProperty("url"),
                props.getProperty("username"),
                props.getProperty("password"));
    }

    Connection getConnection() {
        return connection;
    }

    void closeConnection() throws SQLException {
        if (!this.connection.isClosed()) {
            this.connection.close();
        }
    }

    static void closeAllConnections() throws SQLException {
        for (Map.Entry<ConnDB, Thread> entry : instances.entrySet()) {
            entry.getKey().closeConnection();
        }
    }

    static synchronized ConnDB getInstance(Thread thread) throws Exception {
        if (instances == null) {
            instances = new HashMap<>();
        }
        // 1. проверяем есть ли в пуле объект Thread, если есть то возвращаем объект ConnDB
        Optional<Map.Entry<ConnDB, Thread>> entryOptional = instances.entrySet().stream()
                .filter(el -> el.getValue().equals(thread)).findFirst();
        if (entryOptional.isPresent()) {
            Map.Entry<ConnDB, Thread> entry = entryOptional.get();
            if (!entry.getKey().connection.isClosed()) {
                return entry.getKey();
            }
        }
        // 2. если в пуле мы не нашли наш объект Thread, то проверяем если ли в нем свободный коннект ConnDB
        for (Map.Entry<ConnDB, Thread> entry : instances.entrySet()) {
            if (!entry.getValue().isAlive() && !entry.getKey().connection.isClosed()) {
                entry.setValue(thread);
                return entry.getKey();
            }
        }
        // 3. если свободного коннекта мы не нашли, то создаем его
        ConnDB connDB = new ConnDB();
        instances.put(connDB, thread);
        System.out.println("Connection pull size: " + instances.size());
        return connDB;
    }
}