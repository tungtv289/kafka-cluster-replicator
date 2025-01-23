package vn.ghtk.connect.replicator.sink;

import io.confluent.connect.avro.AvroData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import vn.ghtk.connect.replicator.config.SinkConfig;
import vn.ghtk.connect.replicator.utils.TopicId;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ReplicatorWriter implements SinkWriter {

    private final String taskId;
    private final SinkConfig config;

    public ReplicatorWriter(String taskId, SinkConfig config) {
        this.config = config;
        this.taskId = taskId;
    }

    @Override
    public void writer(Collection<SinkRecord> sinkRecords) throws Exception {
        long startTime = System.currentTimeMillis();
        try {
            final Map<TopicId, BufferedRecords> bufferByTable = new HashMap<>();
            for (SinkRecord record : sinkRecords) {
                final TopicId topicName = new TopicId(record.topic());
                BufferedRecords buffer = bufferByTable.get(topicName);
                if (buffer == null) {
                    buffer = new BufferedRecords(config, topicName);
                    bufferByTable.put(topicName, buffer);
                }
                buffer.add(record);
            }
            for (Map.Entry<TopicId, BufferedRecords> entry : bufferByTable.entrySet()) {
                TopicId topicId = entry.getKey();
                BufferedRecords buffer = entry.getValue();
                log.debug("Flushing records in Replicator Writer for topic ID: {}", topicId);
                buffer.flush();
            }
        } catch (Exception e) {
            log.error("Error during write operation.", e);
            throw e;
        }
        long deltaTime = Math.max(System.currentTimeMillis() - startTime, 1);
        log.info("[Task {}] Process {} records in {} ms, speed {} records/sec", taskId, sinkRecords.size(), deltaTime, (sinkRecords.size() * 1000) / deltaTime);
    }
}
