package vn.ghtk.connect.replicator.model.bulk;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import vn.ghtk.connect.replicator.model.entity.es.Key;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class BulkIndex {

    Key key;
    Gson gson = new Gson();

    public BulkIndex(Key key) {
        this.key = key;
    }

    //{ "index" : { "_index" : "test", "_id" : "1", "routing": "user1"} }
    public String build() {
        Map<String, String> k = new HashMap<>();
        k.put("_index", key.getIndex());
        k.put("_id", key.getId());
        if (StringUtils.isNotBlank(key.getRouting())) {
            k.put("routing", key.getRouting());
        }

        Map<String, Object> data = new HashMap<>();
        data.put("index", k);
        return gson.toJson(data);
    }

}
