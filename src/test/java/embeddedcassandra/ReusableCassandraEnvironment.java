package embeddedcassandra;

public class ReusableCassandraEnvironment {
    private static EmbeddedCassandra embeddedCassandra = null;
    private static String host;
    private static int port;

    private ReusableCassandraEnvironment() {
    }

    public synchronized static void start() {
       if (embeddedCassandra == null) {
           host = "localhost";
           port = 9042;
           embeddedCassandra = new EmbeddedCassandra(host, port);
           embeddedCassandra.start();

           Runtime.getRuntime().addShutdownHook(new Thread(() -> embeddedCassandra.stop()));
       }
    }

    public static String getHost() {
        return host;
    }

    public static int getPort() {
        return port;
    }
}
