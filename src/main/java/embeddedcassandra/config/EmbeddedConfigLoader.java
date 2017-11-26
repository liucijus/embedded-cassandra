package embeddedcassandra.config;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;

public class EmbeddedConfigLoader implements ConfigurationLoader {
    public static final String EMBEDDED_CASSANDRA_PORT = "embedded.cassandra.port";
    public static final String EMBEDDED_CASSANDRA_HOST = "embedded.cassandra.host";

    @Override
    public Config loadConfig() throws ConfigurationException {
        String host = System.getProperty(EMBEDDED_CASSANDRA_HOST);
        int port = Integer.parseInt(System.getProperty(EMBEDDED_CASSANDRA_PORT));
        return new EmbeddedConfig(host, port);
    }
}
