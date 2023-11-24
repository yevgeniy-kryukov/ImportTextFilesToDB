package org.ykryukov.import_text_files_to_db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

class ResourceFileUtil {
    public static Properties getFileProperties(final String fileName) {
        // устанавливаем свойства из файла в ресурсах
        final Properties props = new Properties();
        try (InputStream in = ResourceFileUtil.class.getClassLoader().getResourceAsStream(fileName)) {
            if (in == null) {
                throw new IllegalArgumentException("file 'app.properties' not found!");
            }
            props.load(in);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        // перезаписываем свойства из файла в рабочей директории
        final Properties propsOver = new Properties();
        String userDirectory = System.getProperty("user.dir");
        try (InputStream in = new FileInputStream(userDirectory + "/" + fileName)) {
            propsOver.load(in);
        } catch (FileNotFoundException ignored) {
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        for (String key : propsOver.stringPropertyNames()) {
            props.setProperty(key, propsOver.getProperty(key));
        }
        return props;
    }
}
