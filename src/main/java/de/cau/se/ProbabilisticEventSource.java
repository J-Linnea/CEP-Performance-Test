package de.cau.se;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;
import java.util.Random;

public class ProbabilisticEventSource extends RichParallelSourceFunction<Event> {

    private boolean running = true;

    private final long pause;

    private final List<String> possibleActivities;

    private Random random;

    private int currentCase = 1;

    private final double newCaseProbability;

    private final int maxEvents; // Maximum number of events to generate

    private int eventCount = 0;

    public ProbabilisticEventSource(long pause, final List<String> possibleActivities, final double newCaseProbability,
            int maxEvents) {
        this.pause = pause;
        this.possibleActivities = possibleActivities;
        this.newCaseProbability = newCaseProbability;
        this.maxEvents = maxEvents; // Set the maximum number of events
    }

    @Override
    public void open(Configuration configuration) {
        this.random = new Random();
    }

    public void run(SourceContext<Event> sourceContext) throws Exception {
        while (running && eventCount < maxEvents) {
            String activity = possibleActivities.get((int) (random.nextDouble() * possibleActivities.size()));
            final Event event = new Event("c" + currentCase, activity, "n1", "g1");
            sourceContext.collect(event);
            eventCount++;
            if (random.nextDouble() > 1 - newCaseProbability) {
                this.currentCase++;
            }
            Thread.sleep(pause);
        }
    }

    public void cancel() {
        running = false;
    }
}
