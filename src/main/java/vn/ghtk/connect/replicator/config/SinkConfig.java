package vn.ghtk.connect.replicator.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class SinkConfig extends AbstractConfig {

    public static final String KAFKA_REST_BASE_URL = "kafka.rest.url";

    public static final String KAFKA_REST_CLUSTER_ID = "kafka.rest.clusterId";

    public static final String KAFKA_REST_USERNAME_CONFIG = "kafka.rest.username";
    private static final String KAFKA_REST_USERNAME_DOC = "kafka.rest.username";
    private static final String KAFKA_REST_USERNAME_DISPLAY = "Connection Username";
    private static final String KAFKA_REST_USERNAME_DEFAULT = null;

    public static final String KAFKA_REST_PASSWORD_CONFIG = "kafka.rest.password";
    private static final String KAFKA_REST_PASSWORD_DOC = "kafka.rest.password";
    private static final String KAFKA_REST_PASSWORD_DISPLAY = "Connection Password";
    private static final String KAFKA_REST_PASSWORD_DEFAULT = null;

    public static final String BATCH_SIZE = "batch.size";
    private static final int BATCH_SIZE_DEFAULT = 3000;
    private static final String BATCH_SIZE_DOC =
            "Specifies how many records to attempt to batch together for insertion into the destination"
                    + " topic, when possible.";
    private static final String BATCH_SIZE_DISPLAY = "Batch Size";

    private static final String WRITES_GROUP = "Writes";

    private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

    public static final String SSL_CONFIG_PREFIX = "kafka.rest.https.";
    public static final String SECURITY_PROTOCOL_CONFIG = "elastic.security.protocol";
    private static final String SECURITY_PROTOCOL_DOC = "elastic.https";
    private static final String SECURITY_PROTOCOL_DISPLAY = "Security protocol";
    private static final String SECURITY_PROTOCOL_DEFAULT = SecurityProtocol.PLAINTEXT.name();

    private static final String SSL_GROUP = "Security";
    private static final String CONNECTOR_GROUP = "Connector";

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    private static void addConnectorConfigs(ConfigDef configDef) {
        int order = 0;
        configDef
                .define(
                        KAFKA_REST_BASE_URL,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        KAFKA_REST_BASE_URL,
                        KAFKA_REST_BASE_URL,
                        1,
                        ConfigDef.Width.LONG,
                        KAFKA_REST_BASE_URL
                )
                .define(
                        KAFKA_REST_USERNAME_CONFIG,
                        ConfigDef.Type.STRING,
                        KAFKA_REST_USERNAME_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        KAFKA_REST_USERNAME_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        KAFKA_REST_USERNAME_DISPLAY
                )
                .define(
                        KAFKA_REST_PASSWORD_CONFIG,
                        ConfigDef.Type.STRING,
                        KAFKA_REST_PASSWORD_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        KAFKA_REST_PASSWORD_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        KAFKA_REST_PASSWORD_DISPLAY
                )
                .define(
                        KAFKA_REST_CLUSTER_ID,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        KAFKA_REST_CLUSTER_ID,
                        KAFKA_REST_CLUSTER_ID,
                        1,
                        ConfigDef.Width.LONG,
                        KAFKA_REST_CLUSTER_ID
                ).define(
                BATCH_SIZE,
                ConfigDef.Type.INT,
                BATCH_SIZE_DEFAULT,
                NON_NEGATIVE_INT_VALIDATOR,
                ConfigDef.Importance.MEDIUM,
                BATCH_SIZE_DOC, WRITES_GROUP,
                2,
                ConfigDef.Width.SHORT,
                BATCH_SIZE_DISPLAY
        );
    }

    private static void addSecurityConfigs(ConfigDef configDef) {
        ConfigDef sslConfigDef = new ConfigDef();
        SslConfigs.addClientSslSupport(sslConfigDef);
        int order = 0;
        configDef.define(
                SECURITY_PROTOCOL_CONFIG,
                ConfigDef.Type.STRING,
                SECURITY_PROTOCOL_DEFAULT,
                ConfigDef.CaseInsensitiveValidString.in(
                        Arrays.stream(SecurityProtocol.values())
                                .map(SecurityProtocol::name)
                                .collect(Collectors.toList())
                                .toArray(new String[SecurityProtocol.values().length])
                ),
                ConfigDef.Importance.MEDIUM,
                SECURITY_PROTOCOL_DOC,
                SSL_GROUP,
                ++order,
                ConfigDef.Width.SHORT,
                SECURITY_PROTOCOL_DISPLAY
        );
        configDef.embed(
                SSL_CONFIG_PREFIX, SSL_GROUP, configDef.configKeys().size() + 2, sslConfigDef
        );
    }

    protected static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        addConnectorConfigs(configDef);
        addSecurityConfigs(configDef);
        return configDef;
    }

    public String kafkaRestPassword() {
        return getString(KAFKA_REST_USERNAME_CONFIG);
    }

    public String kafkaRestUsername() {
        return getString(KAFKA_REST_PASSWORD_CONFIG);
    }

    public final String connectorName;
    public final String kafkaRestBaseUrl;
    public final String clusterId;

    public final int batchSize;


    public SinkConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
        kafkaRestBaseUrl = getString(KAFKA_REST_BASE_URL);
        clusterId = getString(KAFKA_REST_CLUSTER_ID);
        connectorName = originals.containsKey("name") ? originals.get("name").toString() : null;
        batchSize = getInt(BATCH_SIZE);
    }
}
