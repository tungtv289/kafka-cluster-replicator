package vn.ghtk.connect.replicator.model.entity.es;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@EqualsAndHashCode
public class IndexableRecord {

    private final Key key;
    private final String payload;
    private final Long version;
}
