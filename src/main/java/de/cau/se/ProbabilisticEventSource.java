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

    public ProbabilisticEventSource(long pause, final List<String> possibleActivities, final double newCaseProbability) {
        this.pause = pause;
        this.possibleActivities = possibleActivities;
        this.newCaseProbability = newCaseProbability;
    }

    @Override
    public void open(Configuration configuration) {
        this.random = new Random();
    }

    public void run(SourceContext<Event> sourceContext) throws Exception {
        while (running) {
            String activity = possibleActivities.get((int) (random.nextDouble() * possibleActivities.size()));
            final Event event = new Event("c" + currentCase, activity, "n1", "g1");
            sourceContext.collect(event);
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
