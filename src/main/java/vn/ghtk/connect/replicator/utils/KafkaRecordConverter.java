package vn.ghtk.connect.replicator.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.avro.AvroData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class KafkaRecordConverter {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static KafkaRecord convert(SinkRecord sinkRecord) throws Exception {
        KafkaRecord kafkaRecord = new KafkaRecord();

        // Convert Key
        if (sinkRecord.key() != null) {
            kafkaRecord.key = new KafkaRecord.Key();
            kafkaRecord.key.data = extractJsonNode(sinkRecord.key(), sinkRecord.keySchema());
            kafkaRecord.key.type = determineType(sinkRecord.keySchema(), sinkRecord.key());
            if (sinkRecord.keySchema() != null) {
                kafkaRecord.key.schema = convertConnectSchemaToAvroSchema(sinkRecord.keySchema());
            }
        }
        // Convert Value
        if (sinkRecord.value() != null) {
            kafkaRecord.value = new KafkaRecord.Value();
            kafkaRecord.value.data = extractJsonNode(sinkRecord.value(), sinkRecord.valueSchema());
            kafkaRecord.value.type = determineType(sinkRecord.valueSchema(), sinkRecord.value());
            if (sinkRecord.valueSchema() != null) {
                kafkaRecord.value.schema = convertConnectSchemaToAvroSchema(sinkRecord.valueSchema());
            }
        }
        return kafkaRecord;
    }

    private static Map<String, Object> structToMap(Struct struct) {
        if (struct == null) {
            return null;
        }

        Map<String, Object> map = new HashMap<>();
        Schema schema = struct.schema();

        for (Field field : schema.fields()) {
            Object value = struct.get(field);
            if (field.schema().type().equals(Schema.Type.STRUCT)) {
                if (!field.schema().name().startsWith("io.debezium.connector")) {
                    Map<String, Object> nestedMap = new HashMap<>();
                    nestedMap.put(field.schema().name(), structToMap((Struct) value));
                    value = nestedMap;
                } else {
                    value = structToMap((Struct) value);
                }
            }  else if (field.schema().isOptional() && value != null) {
                Map<String, Object> optionalMap = new HashMap<>();
                optionalMap.put(field.schema().type().getName(), value);
                value = optionalMap;
            }
            map.put(field.name(), value);
        }

        return map;
    }

    private static Object extractJsonNode(Object data, Schema schema) throws Exception {
        if (data instanceof Struct || schema != null) {
            return objectMapper.valueToTree(structToMap((Struct) data));
        } else if (data instanceof String) {
            // Parse string to JSON if possible
            return (String) data;
        } else {
            // Treat as plain value
            return objectMapper.valueToTree(data);
        }
    }

    private static KafkaRecord.Type determineType(Schema schema, Object data) {
        if (schema == null && data instanceof String) {
            return KafkaRecord.Type.STRING;
        } else if (schema == null && data instanceof byte[]) {
            return KafkaRecord.Type.BINARY;
        } else if (schema != null) {
            return KafkaRecord.Type.AVRO;
        }
        return KafkaRecord.Type.STRING;
    }

    /**
     * Converts a Kafka Connect Schema to an Avro JSON Schema using AvroData.
     *
     * @param connectSchema Kafka Connect Schema
     * @return Avro Schema JSON string
     */
    public static String convertConnectSchemaToAvroSchema(Schema connectSchema) {
        // Initialize AvroData for schema conversions
        AvroData avroData = new AvroData(1); // The parameter specifies the Avro schema version
        // Convert Connect Schema to Avro Schema
        org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(connectSchema);
        // Convert Avro Schema to JSON and return
//        return avroSchema.toString(true); // Pretty-print the JSON
//        return avroSchema.toString(false).replace("\"", "\\\""); // Pretty-print the JSON
        return avroSchema.toString(false); // Pretty-print the JSON
    }

}
