package vn.ghtk.connect.replicator.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import vn.ghtk.connect.replicator.config.SinkConfig;
import vn.ghtk.connect.replicator.utils.KafkaRecord;
import vn.ghtk.connect.replicator.service.kafka.HttpKafkaClientImpl;
import vn.ghtk.connect.replicator.service.kafka.KafkaService;
import vn.ghtk.connect.replicator.sink.metadata.SchemaPair;
import vn.ghtk.connect.replicator.utils.KafkaRecordConverter;
import vn.ghtk.connect.replicator.utils.KafkaRestBatchUpdateException;
import vn.ghtk.connect.replicator.utils.TopicId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
public class BufferedRecords {

    private final TopicId topicName;
    private final SinkConfig config;

    private List<SinkRecord> records = new ArrayList<>();
    private Schema keySchema;
    private Schema valueSchema;

    private final KafkaService kafkaService;

    public BufferedRecords(
            SinkConfig config,
            TopicId topicName
    ) {
        this.topicName = topicName;
        this.config = config;
        this.kafkaService = new HttpKafkaClientImpl(config.kafkaRestBaseUrl, config.clusterId, config.kafkaRestUsername(), config.kafkaRestPassword());

    }

    public List<SinkRecord> add(SinkRecord record) throws Exception {
        final List<SinkRecord> flushed = new ArrayList<>();

        boolean schemaChanged = false;
        if (!Objects.equals(keySchema, record.keySchema())) {
            keySchema = record.keySchema();
            schemaChanged = true;
        }
        if (Objects.equals(valueSchema, record.valueSchema())) {
            flushed.addAll(flush());
        } else {
            // value schema is not null and has changed. This is a real schema change.
            valueSchema = record.valueSchema();
            schemaChanged = true;
        }
        if (schemaChanged) {
            // Each batch needs to have the same schemas, so get the buffered records out
            flushed.addAll(flush());

            // re-initialize everything that depends on the record schema
            final SchemaPair schemaPair = new SchemaPair(
                    record.keySchema(),
                    record.valueSchema()
            );
        }

        records.add(record);

        if (records.size() >= config.batchSize) {
            flushed.addAll(flush());
        }
        return flushed;
    }

    public List<SinkRecord> flush() throws Exception {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        log.debug("Flushing {} buffered records", records.size());
        List<KafkaRecord> kafkaRecords = new ArrayList<>();
        for (SinkRecord record : records) {
            kafkaRecords.add(KafkaRecordConverter.convert(record));
        }
        executeProducer(kafkaRecords);

        final List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        return flushedRecords;
    }

    private void executeProducer(List<KafkaRecord> idxRecords) throws KafkaRestBatchUpdateException, IOException {
        int batchStatus = kafkaService.executeBatch(idxRecords, topicName);
        if (batchStatus != 200) {
            throw new KafkaRestBatchUpdateException(
                    "Execution failed for part of the batch update: " + batchStatus);
        }
    }
}
