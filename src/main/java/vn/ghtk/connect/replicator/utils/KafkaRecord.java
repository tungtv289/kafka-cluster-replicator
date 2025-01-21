package vn.ghtk.connect.replicator.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaRecord {

    // Enum for Type
    public enum Type {
        @JsonProperty("BINARY") BINARY,
        @JsonProperty("JSON") JSON,
        @JsonProperty("AVRO") AVRO,
        @JsonProperty("STRING") STRING
    }

    // Record Key
    public static class Key {
        public Type type; // "BINARY"
        public Object data; // Base64-encoded binary data
    }

    // Record Value
    public static class Value {
        public Type type; // "JSON", "AVRO", "STRING"
        public JsonNode data; // Flexible to handle JSON or plain data
        public Object schema; // Schema for "AVRO"
    }

    public Key key;
    public Value value;


    @Override
    public String toString() {
        return "KafkaRecord{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
