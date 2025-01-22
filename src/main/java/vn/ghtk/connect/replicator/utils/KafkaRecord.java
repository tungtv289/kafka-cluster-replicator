package vn.ghtk.connect.replicator.utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaRecord {
    // Enum for Type
    public enum Type {
        @JsonProperty("BINARY") BINARY,
        @JsonProperty("JSON") JSON,
        @JsonProperty("STRING") STRING,
        @JsonProperty("AVRO") AVRO,
        @JsonProperty("JSONSCHEMA") JSONSCHEMA,
        @JsonProperty("PROTOBUF") PROTOBUF
    }

    // Record Key
    public static class Key {
        public Type type;
        public Object data;  // Flexible to handle JSON or plain data

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String schema; // Optional: for AVRO or schema-based data

        // Custom getter for schema to exclude it when type is "JSON"
        @JsonIgnore
        public String normalizeSchema() {
            return type.equals("JSON") ? null : this.schema;
        }

        // Jackson will still look for this field; we explicitly tell it not to serialize
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String getSchema() {
            return normalizeSchema();
        }
    }

    // Record Value
    public static class Value {
        public Type type;
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
        public String normalizeSchema() {
            return type.equals("JSON") ? null : this.schema;
        }

        // Jackson will still look for this field; we explicitly tell it not to serialize
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String getSchema() {
            return normalizeSchema();
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
