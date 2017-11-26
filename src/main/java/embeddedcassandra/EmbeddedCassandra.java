package embeddedcassandra;

import embeddedcassandra.config.EmbeddedConfigLoader;
import org.apache.cassandra.service.CassandraDaemon;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class EmbeddedCassandra {
    private CassandraDaemon cassandra;
    private CountDownLatch latch = new CountDownLatch(1);
    private int port;
    private String host;

    public EmbeddedCassandra(String host, int port) {
        this.port = port;
        this.host = host;
    }

    public static void main(String[] args) throws IOException {
        new EmbeddedCassandra("127.0.0.1", 9042).start();
    }

    public void start() {
        System.setProperty(EmbeddedConfigLoader.EMBEDDED_CASSANDRA_HOST, host);
        System.setProperty(EmbeddedConfigLoader.EMBEDDED_CASSANDRA_PORT, String.valueOf(port));
        System.setProperty("cassandra.config.loader", EmbeddedConfigLoader.class.getCanonicalName());
        System.setProperty("cassandra-foreground", "true");
        System.setProperty("cassandra.native.epoll.enabled", "false");
        System.setProperty("cassandra.unsafesystem", "true");
        long start = System.currentTimeMillis();

        new Thread(() -> {
            cassandra = new CassandraDaemon(true);
            cassandra.activate();
            latch.countDown();
        }).start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logTime("Started Cassandra", start);
    }

    public void stop() {
        long start = System.currentTimeMillis();
        cassandra.deactivate();
        logTime("Stopped Cassandra", start);
    }

    private void logTime(String message, long start) {
        System.out.println(message + " in " + (System.currentTimeMillis() - start) + " ms");
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }
}



