package vn.ghtk.connect.replicator.service.kafka;

import vn.ghtk.connect.replicator.model.entity.es.IndexableRecord;

import java.io.IOException;
import java.util.List;

public interface KafkaService {

    void execute(List<IndexableRecord> records) throws IOException;

}
