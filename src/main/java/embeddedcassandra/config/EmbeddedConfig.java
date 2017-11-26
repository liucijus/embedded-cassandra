package embeddedcassandra.config;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.locator.SimpleSeedProvider;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.scheduler.RoundRobinScheduler;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.Collections;

import static org.apache.cassandra.config.Config.CommitLogSync.batch;
import static org.apache.cassandra.config.Config.MemtableAllocationType.heap_buffers;

public class EmbeddedConfig extends Config {

    public EmbeddedConfig(String host, int port) {
        super();
        String path = "target/embedded_cassandra/" + Instant.now().toString();
        native_transport_port = port;
        rpc_port = assignPort();
        storage_port = assignPort();
        ssl_storage_port = assignPort();
        memtable_allocation_type = heap_buffers;
        commitlog_sync = batch;
        commitlog_sync_batch_window_in_ms = 1.0;
        commitlog_directory = path + "commitlog";
        hints_directory = path + "hints";
        partitioner = ByteOrderedPartitioner.class.getCanonicalName();
        listen_address = host;
        rpc_address = host;
        start_native_transport = true;
        saved_caches_directory = path + "saved_caches";
        data_file_directories = new String[]{path};
        seed_provider = new ParameterizedClass(
                SimpleSeedProvider.class.getCanonicalName(), Collections.singletonMap("seeds", host)
        );
        endpoint_snitch = SimpleSnitch.class.getCanonicalName();
        dynamic_snitch = true;
        request_scheduler = RoundRobinScheduler.class.getCanonicalName();
        request_scheduler_id = RequestSchedulerId.keyspace;
        incremental_backups = true;
        concurrent_compactors = 4;
        row_cache_class_name = "org.apache.cassandra.cache.OHCProvider";
        row_cache_size_in_mb = 64;
        enable_user_defined_functions = true;
        enable_scripted_user_defined_functions = true;
        cdc_raw_directory = path;
    }

    private int assignPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
