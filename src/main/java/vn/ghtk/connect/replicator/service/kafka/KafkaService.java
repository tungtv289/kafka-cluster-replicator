package vn.ghtk.connect.replicator.service.kafka;

import vn.ghtk.connect.replicator.utils.KafkaRecord;
import vn.ghtk.connect.replicator.utils.TopicId;

import java.io.IOException;
import java.util.List;

public interface KafkaService {

    void execute(KafkaRecord records, TopicId topic) throws IOException;

    int executeBatch(List<KafkaRecord> records, TopicId topic) throws IOException;
}
