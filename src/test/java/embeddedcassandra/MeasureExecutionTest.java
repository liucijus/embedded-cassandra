package embeddedcassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Strings;
import com.google.common.io.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class MeasureExecutionTest {
    private UUID tenantId = UUID.fromString("5ab87f37-3225-4b1a-a4dd-caf1af9ca428");
    private UUID instanceId = UUID.fromString("5020e902-891d-40da-ae35-5dd37950b391");
    private UUID itemId = UUID.fromString("7a8c8a55-f2ce-47be-8c8e-b622b591b7e3");

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

    private void loadSchema(String filename) throws IOException {
        String cql = Resources.toString(Resources.getResource(filename), UTF_8);
        long start = System.currentTimeMillis();
        Session session = cluster.newSession();
        Arrays.stream(cql.split(";"))
                .map(String::trim)
                .filter(query -> !Strings.isNullOrEmpty(query))
                .forEach(query -> {
                    long queryStart = System.currentTimeMillis();
                    session.execute(query);
                    System.out.println("Total Schema load time: " + (System.currentTimeMillis() - queryStart) + " ms");
                });
        System.out.println("Total Schema load time: " + (System.currentTimeMillis() - start) + " ms");
    }

    private void insertData() {
        Session session = cluster.newSession();
        long start = System.currentTimeMillis();
        BoundStatement statement = new BoundStatement(
                session.prepare("" +
                        "INSERT INTO items.items (tenantId, instanceId, itemId, status, commentId, created)\n" +
                        "VALUES (:tenantId, :instanceId, :itemId, :status, :commentId, :created);"
                )
        );
        System.out.println("Prepared INSERT statement in " + (System.currentTimeMillis() - start) + " ms");
        for (int i = 0; i < 100; i++) {
            long startInsert = System.currentTimeMillis();
            statement
                    .setUUID("tenantId", tenantId)
                    .setUUID("instanceId", instanceId)
                    .setUUID("itemId", itemId)
                    .setString("status", "Status")
                    .setUUID("commentId", UUIDs.timeBased())
                    .setLong("created", Instant.now().toEpochMilli());
            session.execute(statement);
            System.out.println("Inserted in " + (System.currentTimeMillis() - startInsert) + " ms");
        }

        System.out.println("Total insert time: " + (System.currentTimeMillis() - start) + " ms");
    }

    private int queryData() {
        Session session = cluster.newSession();
        long start = System.currentTimeMillis();
        BoundStatement statement = new BoundStatement(
                session.prepare("" +
                        "    SELECT commentId" +
                        "      FROM items.items" +
                        "     WHERE tenantId = :tenantId" +
                        "       AND instanceId = :instanceId" +
                        "       AND itemId = :itemId" +
                        "     ORDER BY itemId DESC, commentId DESC;"
                )
        );
        System.out.println("Prepared SELECT statement in " + (System.currentTimeMillis() - start) + " ms");
        statement
                .setUUID("tenantId", tenantId)
                .setUUID("instanceId", instanceId)
                .setUUID("itemId", itemId);

        int size = session.execute(statement).all().size();
        System.out.println("Total query time: " + (System.currentTimeMillis() - start) + " ms");
        return size;
    }

    @After
    public void stopCassandra() {
        cluster.close();
    }

    @Test
    public void insertAndQuery() throws Exception {
        loadSchema("schema.cql");
        insertData();

        assertEquals(100, queryData());
    }
}
