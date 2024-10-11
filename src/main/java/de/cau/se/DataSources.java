package de.cau.se;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class DataSources {

    private StreamExecutionEnvironment env;

    public DataSources(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public DataStreamSource<Event> getExampleDataStream() {
        return env.fromElements(
                new Event("c1", "a1", "n1", "group"),
                new Event("c1", "a2", "n1", "group"),
                new Event("c1", "a3", "n1", "group"),
                new Event("c1", "a4", "n1", "group"),
                new Event("c2", "a1", "n1", "group"),
                new Event("c2", "a2", "n1", "group"),
                new Event("c2", "a2", "n1", "group"),
                new Event("c2", "a3", "n1", "group"),
                new Event("c2", "a4", "n1", "group"),
                new Event("c3", "a1", "n1", "group"),
                new Event("c3", "a2", "n1", "group"),
                new Event("c3", "a3", "n1", "group"),
                new Event("c3", "a4", "n1", "group"),
                new Event("c4", "a2", "n1", "group"),
                new Event("c4", "a3", "n1", "group")
        );
    }

    public DataStreamSource<Event> getProbabilisticDataStream() {
        return env.addSource(new ProbabilisticEventSource(1000, List.of("a1", "a2", "a3"), 0.2));
    }

}
