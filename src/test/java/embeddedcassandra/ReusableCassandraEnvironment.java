package embeddedcassandra;

public class ReusableCassandraEnvironment {
    private static EmbeddedCassandra embeddedCassandra = null;

    private ReusableCassandraEnvironment() {
    }

    public synchronized static void start() {
       if (embeddedCassandra == null) {
           embeddedCassandra = new EmbeddedCassandra("localhost", 9042);
           embeddedCassandra.start();

           Runtime.getRuntime().addShutdownHook(new Thread(() -> embeddedCassandra.stop()));
       }
    }

}
