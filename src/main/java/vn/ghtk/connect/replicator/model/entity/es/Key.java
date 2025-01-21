package vn.ghtk.connect.replicator.model.entity.es;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@EqualsAndHashCode
public class Key {

    private final String index;
    private final String type;
    private final String id;
    private String routing;

    public Key(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

}
