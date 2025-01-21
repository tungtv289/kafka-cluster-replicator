package vn.ghtk.connect.replicator.utils;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class KafkaRecordConverter {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static KafkaRecord convert(SinkRecord sinkRecord) throws Exception {
        KafkaRecord kafkaRecord = new KafkaRecord();

        // Convert Key
        if (sinkRecord.key() != null) {
            kafkaRecord.key = new KafkaRecord.Key();
            kafkaRecord.key.data = extractData(sinkRecord.key(), sinkRecord.keySchema());
            kafkaRecord.key.type = determineType(sinkRecord.keySchema(), sinkRecord.key());
        }

        // Convert Value
        if (sinkRecord.value() != null) {
            kafkaRecord.value = new KafkaRecord.Value();
            kafkaRecord.value.data = extractJsonNode(sinkRecord.value(), sinkRecord.valueSchema());
            kafkaRecord.value.type = determineType(sinkRecord.valueSchema(), sinkRecord.value());

            if (sinkRecord.valueSchema() != null) {
                kafkaRecord.value.schema = sinkRecord.valueSchema().toString(); // Raw schema if available
            }
        }

        return kafkaRecord;
    }

    private static String extractData(Object data, Schema schema) {
        if (data instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) data);
        } else if (data instanceof String) {
            return (String) data;
        } else {
            return data.toString(); // Fallback for other types
        }
    }

    private static JsonNode extractJsonNode(Object data, Schema schema) throws Exception {
        if (data instanceof Struct || schema != null) {
            Struct struct = (Struct) data;
            Map<String, Object> structData = new HashMap<>();

            // Extract all fields from the Struct
            for (Field field : schema.fields()) {
                Object fieldValue = struct.get(field);
                structData.put(field.name(), fieldValue);
            }

            // Convert the Struct to a JSON-friendly Map
            return objectMapper.valueToTree(structData);
        } else if (data instanceof String) {
            // Parse string to JSON if possible
            return objectMapper.readTree((String) data);
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
        } else if (schema != null && "AVRO".equals(schema.name())) {
            return KafkaRecord.Type.AVRO;
        } else if (schema != null) {
            return KafkaRecord.Type.JSON; // Assume JSON for other structured schemas
        }
        return KafkaRecord.Type.STRING; // Default fallback
    }
}
