package vn.ghtk.connect.replicator;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import vn.ghtk.connect.replicator.config.SinkConfig;
import vn.ghtk.connect.replicator.config.Version;
import vn.ghtk.connect.replicator.sink.ReplicatorWriter;

import java.util.Collection;
import java.util.Map;

public class ReplicatorSinkTask extends SinkTask {

    private SinkConfig config;
    private ReplicatorWriter sinkWriter;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        String taskId = map.get("task_id");
        config = new SinkConfig(map);
        initWriter(taskId);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            return;
        }
        sinkWriter.writer(collection);
    }

    @Override
    public void stop() {

    }

    private void initWriter(String taskId) {
        sinkWriter = new ReplicatorWriter(taskId, config);
    }
}
