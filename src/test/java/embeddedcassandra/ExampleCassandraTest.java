package embeddedcassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.junit.Assert.assertEquals;

public class ExampleCassandraTest {
    private String host = "127.0.0.1";
    private Cluster cluster;

    @Before
    public void startCassandra() throws IOException {
        ReusableCassandraEnvironment.start();
        cluster = Cluster.builder()
                .withPort(9042)
                .addContactPoint(host)
                .build();
    }

    @After
    public void stopCassandra() {
        cluster.close();
    }

    @Test
    public void canQuery() throws Exception {
        ResultSet resultSet = cluster.newSession().execute(
                select().all().from("system", "local")
        );

        assertEquals(host, resultSet.one().getInet("listen_address").getHostAddress());
    }
}
