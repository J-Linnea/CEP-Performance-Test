package de.cau.se;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Event {
    private String caseId;
    private String activity;
    private long timestamp;
    private String node;
    private String group;

    public Event() {
    }

    public Event(
            final String caseId,
            final String activity,
            final String node,
            final String group
    ) {
        this.caseId = caseId;
        this.activity = activity;
        this.node = node;
        this.group = group;
        this.timestamp = System.currentTimeMillis();
    }

    public void setCaseId(String caseId) {
        this.caseId = caseId;
    }

    public void setActivity(String activity) {
        this.activity = activity;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getCaseId() {
        return caseId;
    }

    public String getNode() {
        return node;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getActivity() {
        return activity;
    }

    public String getGroup() {
        return group;
    }


    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(caseId)
                .append(activity)
                .append(timestamp)
                .append(node)
                .append(group)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Event) {
            final Event other = (Event) obj;
            return new EqualsBuilder()
                    .append(caseId, other.caseId)
                    .append(activity, other.activity)
                    .append(timestamp, other.timestamp)
                    .append(node, other.node)
                    .append(group, other.group)
                    .isEquals();
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "Event{" +
                "caseId='" + caseId + '\'' +
                ", activity='" + activity + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", location='" + node + '\'' +
                '}';
    }
}

