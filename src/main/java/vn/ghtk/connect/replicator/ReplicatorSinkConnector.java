package vn.ghtk.connect.replicator;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import vn.ghtk.connect.replicator.config.SinkConfig;
import vn.ghtk.connect.replicator.config.Version;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplicatorSinkConnector extends SinkConnector {

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> map) {
        configProps = map;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ReplicatorSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        final List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskProps = new HashMap<>();
            taskProps.putAll(configProps);
            taskProps.put("task_id", String.valueOf(i));
            configs.add(taskProps);
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return SinkConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
