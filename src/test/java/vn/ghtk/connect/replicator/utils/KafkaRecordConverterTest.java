package vn.ghtk.connect.replicator.utils;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class KafkaRecordConverterTest {

    @Test
    void testConvertWithStringKeyAndValue() throws Exception {
        SinkRecord sinkRecord = mock(SinkRecord.class);
        when(sinkRecord.key()).thenReturn("test-key");
        when(sinkRecord.value()).thenReturn("test-value");

        KafkaRecord result = KafkaRecordConverter.convert(sinkRecord);

        assertNotNull(result);
        assertEquals(KafkaRecord.Type.STRING, result.key.type);
        assertEquals(KafkaRecord.Type.STRING, result.value.type);

        assertEquals("test-key", result.key.data);
        assertEquals("test-value", result.value.data);
    }

    @Test
    void testConvertWithStructKeyStructValue() throws Exception {
        SinkRecord sinkRecord = mock(SinkRecord.class);

        Schema valueSchema = SchemaBuilder.struct()
                .name("anhhd25_avro.master_test.pick_addrs.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("pick_tel", SchemaBuilder.string().defaultValue("").build())
                .field("pick_test_1", SchemaBuilder.string().defaultValue("").build())
                .field("pick_test_2", Schema.OPTIONAL_STRING_SCHEMA)
                .field("pick_test_3", SchemaBuilder.string().defaultValue("").build())
                .field("pick_test_4", SchemaBuilder.string().defaultValue("").build())
                .field("pick_test_5", SchemaBuilder.string().defaultValue("").build())
                .field("pick_test_6", SchemaBuilder.string().defaultValue("").build())
                .field("pick_test_7", SchemaBuilder.string().defaultValue("").build())
                .optional()
                .build();

        Schema sourceSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.mysql.Source")
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("snapshot", Schema.OPTIONAL_STRING_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ts_us", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
                .field("table", Schema.OPTIONAL_STRING_SCHEMA)
                .field("server_id", Schema.INT64_SCHEMA)
                .field("gtid", Schema.OPTIONAL_STRING_SCHEMA)
                .field("file", Schema.STRING_SCHEMA)
                .field("pos", Schema.INT64_SCHEMA)
                .field("row", Schema.INT32_SCHEMA)
                .field("thread", Schema.OPTIONAL_INT64_SCHEMA)
                .field("query", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema transactionSchema = SchemaBuilder.struct()
                .name("event.block")
                .field("id", Schema.STRING_SCHEMA)
                .field("total_order", Schema.INT64_SCHEMA)
                .field("data_collection_order", Schema.INT64_SCHEMA)
                .optional()
                .build();

        Schema envelopeSchema = SchemaBuilder.struct()
                .name("anhhd25_avro.master_test.pick_addrs.Envelope")
                .field("before", valueSchema)
                .field("after", valueSchema)
                .field("source", sourceSchema)
                .field("transaction", transactionSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_us", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        Struct beforeValue = new Struct(valueSchema)
                .put("id", 1)
                .put("pick_tel", "123456789")
                .put("pick_test_1", "test1")
                .put("pick_test_2", null)
                .put("pick_test_3", "test3")
                .put("pick_test_4", "test4")
                .put("pick_test_5", "test5")
                .put("pick_test_6", "test6")
                .put("pick_test_7", "test7");

        Struct sourceValue = new Struct(sourceSchema)
                .put("version", "1.9")
                .put("connector", "mysql")
                .put("name", "db-server-1")
                .put("ts_ms", 1700000000000L)
                .put("snapshot", "false")
                .put("db", "test_db")
                .put("sequence", null)
                .put("ts_us", null)
                .put("ts_ns", null)
                .put("table", "test_table")
                .put("server_id", 123456L)
                .put("gtid", null)
                .put("file", "binlog.000001")
                .put("pos", 456L)
                .put("row", 1)
                .put("thread", null)
                .put("query", null);

        Struct envelope = new Struct(envelopeSchema)
                .put("before", beforeValue)
                .put("after", null)
                .put("source", sourceValue)
                .put("transaction", null)
                .put("op", "c")
                .put("ts_ms", 1700000000000L)
                .put("ts_us", null)
                .put("ts_ns", null);

        System.out.println(envelope);

        when(sinkRecord.valueSchema()).thenReturn(envelopeSchema);
        when(sinkRecord.value()).thenReturn(envelope);

        KafkaRecord result = KafkaRecordConverter.convert(sinkRecord);

        assertNotNull(result);
    }
        @Test
    void testConvertWithStructKeyAndValue() throws Exception {
        Schema keySchema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();

        Struct keyStruct = new Struct(keySchema)
                .put("field1", "value1")
                .put("field2", 123);

        Schema valueSchema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();

        Struct valueStruct = new Struct(keySchema)
                .put("field1", "value1")
                .put("field2", 123);

        SinkRecord sinkRecord = mock(SinkRecord.class);
        when(sinkRecord.keySchema()).thenReturn(keySchema);
        when(sinkRecord.key()).thenReturn(keyStruct);

        when(sinkRecord.valueSchema()).thenReturn(valueSchema);
        when(sinkRecord.value()).thenReturn(valueStruct);

        KafkaRecord result = KafkaRecordConverter.convert(sinkRecord);

        assertNotNull(result);
        assertEquals(KafkaRecord.Type.JSON, result.key.type);
        assertEquals(KafkaRecord.Type.JSON, result.value.type);

        JsonNode keyNode = (JsonNode) result.key.data;
        assertEquals("value1", keyNode.get("field1").asText());
        assertEquals(123, keyNode.get("field2").asInt());
    }
}
