package vn.ghtk.connect.replicator.service.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import vn.ghtk.connect.replicator.service.http.HttpResponseText;
import vn.ghtk.connect.replicator.service.http.HttpService;
import vn.ghtk.connect.replicator.service.http.HttpServiceImpl;
import vn.ghtk.connect.replicator.utils.KafkaRecord;
import vn.ghtk.connect.replicator.utils.TopicId;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

@Slf4j
public class HttpKafkaClientImpl implements KafkaService {

    private final String baseUrl;
    private final String clusterId;
    private final String username;
    private final String password;
    private final HttpService httpService = new HttpServiceImpl();

    private static final ObjectMapper objectMapper = new ObjectMapper();


    public HttpKafkaClientImpl(String host, String clusterId, String username, String password) {
        this.baseUrl = host;
        this.clusterId = clusterId;
        this.username = username;
        this.password = password;
    }

    @Override
    public void execute(KafkaRecord records, TopicId topic) throws IOException {
    }

    @Override
    public int executeBatch(List<KafkaRecord> records, TopicId topic) throws IOException {
//        String endpoint = String.format("%s/v3/clusters/%s/topics/%s/records", baseUrl, clusterId, topic.topicName());
        String endpoint = String.format("%s/v3/clusters/%s/topics/%s/records", baseUrl, clusterId, "admin_service_db_avro");
        log.info(endpoint);
        // Prepare payload
        KafkaRestV3Payload payload = new KafkaRestV3Payload(records);
        // Serialize payload to JSON
        String jsonPayload = objectMapper.writeValueAsString(payload);
        log.info(jsonPayload);
        // Prepare HTTP POST request
        HttpPost httpRequest = new HttpPost(endpoint);
        httpRequest.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON.withCharset("utf-8")));

        String encoding = Base64.getEncoder().encodeToString((username + ":" + password).getBytes("UTF-8"));
        httpRequest.setHeader("Authorization", "Basic " + encoding);
        httpRequest.setHeader("Content-Type", "application/json");

        HttpResponseText httpData = httpService.execute(httpRequest);
        return httpData.getCode();
    }

    // Payload wrapper for REST Proxy API v3
    static class KafkaRestV3Payload {
        public final List<KafkaRecord> entries;

        public KafkaRestV3Payload(List<KafkaRecord> entries) {
            this.entries = entries;
        }
    }
}
