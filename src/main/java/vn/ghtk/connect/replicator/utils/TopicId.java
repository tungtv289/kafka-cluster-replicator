package vn.ghtk.connect.replicator.utils;


import java.util.Objects;

public class TopicId implements Comparable<TopicId> {
    private final String topicName;
    private final int hash;

    public TopicId(String topicName) {
        this.topicName = topicName;
        this.hash = Objects.hash(topicName);
    }

    public String topicName() {
        return topicName;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof TopicId) {
            TopicId that = (TopicId) obj;
            return Objects.equals(this.topicName, that.topicName);
        }
        return false;
    }

    @Override
    public int compareTo(TopicId that) {
        if (that == this) {
            return 0;
        }
        int diff = this.topicName.compareTo(that.topicName);
        if (diff != 0) {
            return diff;
        }
        return 0;
    }
}
