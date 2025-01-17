package de.cau.se;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class CepPerformanceTest {

    public int matches;

    public static void main(String[] args) throws Exception {
        // Setup Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSources dataSources = new DataSources(env);

        DataStream<Event> inputEventStream = dataSources.getProbabilisticDataStream()
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(
                                (event, l) -> event.getTimestamp()));

        // Define the number of parallel queries to test
        int numQueries = 500; // Set this to a high number for stress testing

        // Output tag for timeout events
        final OutputTag<String> timeoutTag = new OutputTag<String>("alarm") {
        };

        // Collecting all query results
        // DataStream<String> cepResult = null;

        // Applying multiple parallel CEP queries
        for (int i = 0; i < numQueries; i++) {
            final int queryIndex = i;

            Pattern<Event, ?> alarmPattern = Pattern.<Event>begin("first_" + queryIndex)
                    .where(SimpleCondition.of(evt -> evt.getActivity().equals("A")))
                    .next("second_" + queryIndex)
                    .where(SimpleCondition.of(evt -> evt.getActivity().equals("B")))
                    .within(Time.seconds(1));

            // Query 5: Absence(A)

            // Pattern<Event, ?> alarmPattern = Pattern.<Event>begin("first_" + queryIndex)
            // .where(SimpleCondition.of(event -> !event.getActivity().equals("A")))
            // .notFollowedBy("A_" + queryIndex)
            // .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
            // .within(Time.seconds(3));

            DataStream<String> singleQueryResult = CEP.pattern(inputEventStream,
                    alarmPattern).select(
                            timeoutTag,
                            (PatternTimeoutFunction<Event, String>) (pattern, timeout) -> {
                                return pattern.get("first_" + queryIndex).toString() + " - TIMEOUT!";
                            },
                            (PatternSelectFunction<Event, String>) pattern -> {
                                return "ALARM!!! " + pattern.get("first_" + queryIndex).toString();
                            });

            // Combine results
            // if (cepResult == null) {
            // cepResult = singleQueryResult;
            // } else {
            // cepResult = cepResult.union(singleQueryResult, singleQueryResult2);
            // }
        }

        // Execute the Flink job
        long startTime = System.currentTimeMillis();
        env.execute("CepPerformanceTest");
        long endTime = System.currentTimeMillis();

        // Calculate and print the total execution time
        long executionTime = endTime - startTime;
        System.out.println("Total execution time: " + executionTime + " ms");

    }

}
