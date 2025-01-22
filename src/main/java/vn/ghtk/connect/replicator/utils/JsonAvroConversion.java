package vn.ghtk.connect.replicator.utils;

import com.fasterxml.jackson.databind.JsonNode;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;

import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;

interface MessageFormatter {

    String format(String topic, byte[] value);

    static Map<SchemaType, MessageFormatter> createMap(SchemaRegistryClient schemaRegistryClient) {
        return new HashMap<SchemaType, MessageFormatter>() {{
            put(SchemaType.AVRO, new AvroMessageFormatter(schemaRegistryClient));
            put(SchemaType.JSON, new JsonSchemaMessageFormatter(schemaRegistryClient));
        }};
    }

    class AvroMessageFormatter implements MessageFormatter {
        private final KafkaAvroDeserializer avroDeserializer;

        AvroMessageFormatter(SchemaRegistryClient client) {
            this.avroDeserializer = new KafkaAvroDeserializer(client);
        }

        @Override
        @SneakyThrows
        public String format(String topic, byte[] value) {
            // deserialized will have type, that depends on schema type (record or primitive),
            // AvroSchemaUtils.toJson(...) method will take it into account
            Object deserialized = avroDeserializer.deserialize(topic, value);
            byte[] jsonBytes = AvroSchemaUtils.toJson(deserialized);
            return new String(jsonBytes);
        }
    }

    class JsonSchemaMessageFormatter implements MessageFormatter {
        private final KafkaJsonSchemaDeserializer<JsonNode> jsonSchemaDeserializer;

        JsonSchemaMessageFormatter(SchemaRegistryClient client) {
            this.jsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(client);
        }

        @Override
        public String format(String topic, byte[] value) {
            JsonNode json = jsonSchemaDeserializer.deserialize(topic, value);
            return json.toString();
        }
    }
}
