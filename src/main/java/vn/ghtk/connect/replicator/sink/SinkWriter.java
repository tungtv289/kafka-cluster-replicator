package vn.ghtk.connect.replicator.sink;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

public interface SinkWriter {

    void writer(Collection<SinkRecord> sinkRecords) throws Exception;
}
