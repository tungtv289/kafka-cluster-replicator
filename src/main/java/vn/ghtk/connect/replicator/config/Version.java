package vn.ghtk.connect.replicator.config;

import java.io.InputStream;
import java.util.Properties;

public class Version {

    private static final String PATH_VERSION = "version.properties";
    private static String version = "unknown";

    public static String getVersion() {
        try {
            InputStream stream = Version.class.getResourceAsStream(PATH_VERSION);
            Properties properties = new Properties();
            properties.load(stream);
            version = properties.getProperty("version", version).trim();
            return version;
        } catch (Exception e) {
            return version;
        }
    }
}
