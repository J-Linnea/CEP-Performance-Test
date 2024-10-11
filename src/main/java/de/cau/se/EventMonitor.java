package de.cau.se;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class EventMonitor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSources dataSources = new DataSources(env);

        DataStream<Event> inputEventStream =
                dataSources.getExampleDataStream()
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(
                                (event, l) -> event.getTimestamp()
                        )
                );

        inputEventStream.print();

        Pattern<Event, ?> alarmPattern = Pattern.<Event>begin("first")
                .where(SimpleCondition.of(evt -> evt.getActivity().equals("a1")))
                .next("second")
                .where(SimpleCondition.of(evt -> evt.getActivity().equals("a2")))
                .within(Time.seconds(10));

        DataStream<String> cepResult = CEP.pattern(inputEventStream, alarmPattern).
                select(
                        new OutputTag<>("alarm", TypeInformation.of(String.class)),
                        (PatternTimeoutFunction<Event, String>) (pattern, l) -> pattern.get("first").toString(),
                        (PatternSelectFunction<Event, String>) pattern -> "ALARM!!!" + pattern.get("first").toString());
        cepResult.print();

        env.execute("CEP monitoring job");
    }
}