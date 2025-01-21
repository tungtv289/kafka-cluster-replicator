package vn.ghtk.connect.replicator.utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
        public Object data; // Flexible to handle JSON or plain data

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String schema; // Optional: for AVRO or schema-based data

        public Value() {
        }

        public Value(Type type, Object data, String schema) {
            this.type = type;
            this.data = data;
            this.schema = schema;
        }

        // Custom getter for schema to exclude it when type is "JSON"
        @JsonIgnore
        public String getSchema() {
            return type.equals("JSON") ? null : this.schema;
        }

        // Jackson will still look for this field; we explicitly tell it not to serialize
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String getSerializedSchema() {
            return getSchema();
        }

    }

    public String id;
    public Key key;
    public Value value;


    @Override
    public String toString() {
        return "KafkaRecord{" +
                "id=" + id +
                ", key=" + key +
                ", value=" + value +
                '}';
    }
}
