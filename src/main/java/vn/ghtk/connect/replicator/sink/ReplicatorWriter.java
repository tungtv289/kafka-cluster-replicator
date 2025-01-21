package vn.ghtk.connect.replicator.sink;

import com.google.gson.Gson;
import io.confluent.connect.avro.AvroData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import vn.ghtk.connect.replicator.config.SinkConfig;
import vn.ghtk.connect.replicator.constants.StringConstants;
import vn.ghtk.connect.replicator.model.entity.es.IndexableRecord;
import vn.ghtk.connect.replicator.model.entity.es.Key;
import vn.ghtk.connect.replicator.utils.DateTimeUtils;
import vn.ghtk.connect.replicator.utils.TopicId;

import java.util.*;

@Slf4j
public class ReplicatorWriter implements SinkWriter {

    private final String taskId;
    private final SinkConfig config;
    private final Gson gson = new Gson();

    public ReplicatorWriter(String taskId, SinkConfig config) {
        this.config = config;
        this.taskId = taskId;
    }

    @Override
    public void writer(Collection<SinkRecord> sinkRecords) {
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
//            throw e;
        }
        long deltaTime = Math.max(System.currentTimeMillis() - startTime, 1);
        log.info("[Task {}] Process {} records in {} ms, speed {} records/sec", taskId, sinkRecords.size(), deltaTime, (sinkRecords.size() * 1000) / deltaTime);
    }

    private void handleMessages(Collection<SinkRecord> sinkRecords) {
        List<IndexableRecord> indexableRecords = new ArrayList<>();
        for (SinkRecord sinkRecord : sinkRecords) {
            if (!StringConstants.TOPIC_MESSAGES.equals(sinkRecord.topic())) {
                continue;
            }
            JSONObject jsonObject = new JSONObject(sinkRecord.value().toString());
            JSONObject message = jsonObject.getJSONObject("data").getJSONObject("message");

            String messageId = message.getString("id");
            String channelId = message.getString("channel_id");
            String msgType = message.getString("msg_type");
            String text = message.optString("text");
            String created_at = message.getString("created_at");
            if (StringUtils.isBlank(text)) {
                continue;
            }

            String created = DateTimeUtils.convertTimestamp(DateTimeUtils.convertZuluTime2Date(created_at));
            String partition = created.substring(0, 7).replace("-", "");


            Key key = new Key("messages-" + partition, "_doc", String.valueOf(messageId), channelId);
            IndexableRecord indexableRecord = new IndexableRecord(key, "gson.toJson(messageES)", 1L);
            indexableRecords.add(indexableRecord);
        }
        if (CollectionUtils.isEmpty(indexableRecords)) {
            return;
        }
        try {
//            kafkaService.execute(indexableRecords);
        } catch (Exception e) {
            throw new ConnectException(e);
        }
    }

    /**
     * Converts a Kafka Connect Schema to an Avro JSON Schema using AvroData.
     *
     * @param connectSchema Kafka Connect Schema
     * @return Avro Schema JSON string
     */
    private String convertConnectToAvro(Schema connectSchema) {
        // Initialize AvroData for schema conversions
        AvroData avroData = new AvroData(1); // The parameter specifies the Avro schema version
        // Convert Connect Schema to Avro Schema
        org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(connectSchema);
        // Convert Avro Schema to JSON and return
//        return avroSchema.toString(true); // Pretty-print the JSON
        return avroSchema.toString(false).replace("\"", "\\\""); // Pretty-print the JSON
//        return avroSchema.toString(false); // Pretty-print the JSON
    }
}
