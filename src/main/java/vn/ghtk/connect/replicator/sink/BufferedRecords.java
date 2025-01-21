package vn.ghtk.connect.replicator.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import vn.ghtk.connect.replicator.config.SinkConfig;
import vn.ghtk.connect.replicator.service.kafka.HttpKafkaClientImpl;
import vn.ghtk.connect.replicator.service.kafka.KafkaService;
import vn.ghtk.connect.replicator.sink.metadata.SchemaPair;
import vn.ghtk.connect.replicator.utils.TopicId;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

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
        this.kafkaService = new HttpKafkaClientImpl(config.kafkaRestBaseUrl, config.kafkaRestUsername(), config.kafkaRestPassword());

    }

    public List<SinkRecord> add(SinkRecord record) throws Exception {
        final List<SinkRecord> flushed = new ArrayList<>();

        boolean schemaChanged = false;
        if (!Objects.equals(keySchema, record.keySchema())) {
            keySchema = record.keySchema();
            schemaChanged = true;
        }
        if (Objects.equals(valueSchema, record.valueSchema())) {
            if (config.deleteEnabled && deletesInBatch) {
                // flush so an insert after a delete of same record isn't lost
                flushed.addAll(flush());
            }
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

            final String insertSql = getInsertSql();
            final String deleteSql = getDeleteSql();
            log.debug(
                    "{} sql: {} deleteSql: {} meta: {}",
                    config.insertMode,
                    insertSql,
                    deleteSql,
                    fieldsMetadata
            );
            close();
            updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
            updateStatementBinder = dbDialect.statementBinder(
                    updatePreparedStatement,
                    config.pkMode,
                    schemaPair,
                    fieldsMetadata,
                    dbStructure.tableDefinition(connection, topicName),
                    config.insertMode
            );
            if (config.deleteEnabled && nonNull(deleteSql)) {
                deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
                deleteStatementBinder = dbDialect.statementBinder(
                        deletePreparedStatement,
                        config.pkMode,
                        schemaPair,
                        fieldsMetadata,
                        dbStructure.tableDefinition(connection, topicName),
                        config.insertMode
                );
            }
        }

        // set deletesInBatch if schema value is not null
        if (isNull(record.value()) && config.deleteEnabled) {
            deletesInBatch = true;
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
        for (SinkRecord record : records) {
            updateStatementBinder.bindRecord(record);
        }
        executeUpdates();

        final List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        deletesInBatch = false;
        return flushedRecords;
    }

    private void executeUpdates() throws SQLException {
        kafkaService.execute();
        int[] batchStatus = updatePreparedStatement.executeBatch();
        for (int updateCount : batchStatus) {
            if (updateCount == Statement.EXECUTE_FAILED) {
                throw new BatchUpdateException(
                        "Execution failed for part of the batch update", batchStatus);
            }
        }
    }

    private String getInsertSql() throws SQLException {
        switch (config.insertMode) {
            case INSERT:
                return dbDialect.buildInsertStatement(
                        topicName,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames),
                        dbStructure.tableDefinition(connection, topicName)
                );
            case UPSERT:
                if (fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                                    + " primary key configuration",
                            topicName
                    ));
                }
                try {
                    return dbDialect.buildUpsertQueryStatement(
                            topicName,
                            asColumns(fieldsMetadata.keyFieldNames),
                            asColumns(fieldsMetadata.nonKeyFieldNames),
                            dbStructure.tableDefinition(connection, topicName)
                    );
                } catch (UnsupportedOperationException e) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
                            topicName,
                            dbDialect.name()
                    ));
                }
            case UPDATE:
                return dbDialect.buildUpdateStatement(
                        topicName,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames),
                        dbStructure.tableDefinition(connection, topicName)
                );
            default:
                throw new ConnectException("Invalid insert mode");
        }
    }

    private String getDeleteSql() {
        String sql = null;
        if (config.deleteEnabled) {
            switch (config.pkMode) {
                case RECORD_KEY:
                    if (fieldsMetadata.keyFieldNames.isEmpty()) {
                        throw new ConnectException("Require primary keys to support delete");
                    }
                    try {
                        sql = dbDialect.buildDeleteStatement(
                                topicName,
                                asColumns(fieldsMetadata.keyFieldNames)
                        );
                    } catch (UnsupportedOperationException e) {
                        throw new ConnectException(String.format(
                                "Deletes to table '%s' are not supported with the %s dialect.",
                                topicName,
                                dbDialect.name()
                        ));
                    }
                    break;

                default:
                    throw new ConnectException("Deletes are only supported for pk.mode record_key");
            }
        }
        return sql;
    }

    private Collection<ColumnId> asColumns(Collection<String> names) {
        return names.stream()
                .map(name -> new ColumnId(topicName, name))
                .collect(Collectors.toList());
    }
}
