package vn.ghtk.connect.replicator.service.kafka;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.kafka.connect.errors.ConnectException;
import org.json.JSONObject;
import vn.ghtk.connect.replicator.model.bulk.BulkIndex;
import vn.ghtk.connect.replicator.model.entity.es.IndexableRecord;
import vn.ghtk.connect.replicator.service.http.HttpResponseText;
import vn.ghtk.connect.replicator.service.http.HttpService;
import vn.ghtk.connect.replicator.service.http.HttpServiceImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class HttpKafkaClientImpl implements KafkaService {

    private final String baseUrl;
    private final String username;
    private final String password;
    private final HttpService httpService = new HttpServiceImpl();

    public HttpKafkaClientImpl(String host, String username, String password) {
        this.baseUrl = host;
        this.username = username;
        this.password = password;
    }

    @Override
    public void execute(List<IndexableRecord> records) throws IOException {
        List<String> bulk = createBulkRequest(records);
        if (CollectionUtils.isEmpty(bulk)) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (String line : bulk) {
            JSONObject jsonObject = new JSONObject(line);
            line = jsonObject.toString().replace("\n", "\\n");
            sb.append(line);
            sb.append("\n");
        }

        String encoding = Base64.getEncoder().encodeToString((username + ":" + password).getBytes("UTF-8"));

        HttpUriRequest httpRequest = new HttpPost(baseUrl + "/topics");
        EntityBuilder entityBuilder = EntityBuilder.create()
                .setText(sb.toString())
                .setContentType(ContentType.APPLICATION_JSON.withCharset("utf-8"));
        ((HttpEntityEnclosingRequest) httpRequest).setEntity(entityBuilder.build());
        httpRequest.setHeader("Authorization", "Basic " + encoding);
        httpRequest.setHeader("Content-Type", "application/vnd.kafka.avro.v2+json");

        HttpResponseText httpData = httpService.execute(httpRequest);
        if (httpData.getCode() < 200 || httpData.getCode() >= 300) {
            throw new ConnectException(httpData.getText());
        }
    }

    public List<String> createBulkRequest(List<IndexableRecord> batch) {
        if (CollectionUtils.isEmpty(batch)) {
            return new ArrayList<>();
        }
        List<String> bulk = new ArrayList<>();
        for (IndexableRecord record : batch) {
            bulk.addAll(toBulkAction(record));
        }
        return bulk;
    }

    private List<String> toBulkAction(IndexableRecord record) {
        BulkIndex bulkIndex = new BulkIndex(record.getKey());
        List<String> bulk = new ArrayList<>();
        bulk.add(bulkIndex.build());
        bulk.add(record.getPayload());
        return bulk;
    }
}
