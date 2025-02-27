package vn.ghtk.connect.replicator.utils;
import java.util.Optional;
import org.apache.commons.lang3.EnumUtils;

enum SchemaType {
    AVRO,
    JSON,
    PROTOBUF;

    public static Optional<SchemaType> fromString(String typeString) {
        return Optional.ofNullable(EnumUtils.getEnum(SchemaType.class, typeString));
    }
}
