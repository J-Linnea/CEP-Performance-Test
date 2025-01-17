package de.cau.se;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class EventMonitor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSources dataSources = new DataSources(env);

        DataStream<Event> inputEventStream = dataSources.getProbabilisticDataStream()
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(
                                (event, l) -> event.getTimestamp()));

        inputEventStream.print();

        /*
         * Example Pattern:
         * Pattern<Event, ?> alarmPattern = Pattern.<Event>begin("first")
         * .where(SimpleCondition.of(evt -> evt.getActivity().equals("a1")))
         * .next("second")
         * .where(SimpleCondition.of(evt -> evt.getActivity().equals("a2")))
         * .within(Time.seconds(10));
         * 
         * DataStream<String> cepResult = CEP.pattern(inputEventStream,
         * alarmPattern).select(
         * new OutputTag<>("alarm", TypeInformation.of(String.class)),
         * (PatternTimeoutFunction<Event, String>) (pattern, l) ->
         * pattern.get("first").toString(),
         * (PatternSelectFunction<Event, String>) pattern -> "ALARM!!!" +
         * pattern.get("first").toString());
         * cepResult.print();
         */

        // Query 1: Init(A)

        Pattern<Event, ?> queryInit = Pattern.<Event>begin("init")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .within(Time.seconds(10));

        // Query 2: Existence(A)

        Pattern<Event, ?> queryExistence = Pattern.<Event>begin("existence")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .oneOrMore() // Mehrfaches Auftreten möglich, aber mindestens einmal.
                .within(Time.seconds(10));

        // Query 3: Existence2(A)

        Pattern<Event, ?> queryExistence2 = Pattern.<Event>begin("A1")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .followedBy("A2")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .oneOrMore()
                .within(Time.seconds(10));

        // Query 4: Existence3(A)

        Pattern<Event, ?> queryExistence3 = Pattern.<Event>begin("A1")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .followedBy("A2")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .followedBy("A3")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .oneOrMore()
                .within(Time.seconds(10));

        // Query 5: Absence(A)

        Pattern<Event, ?> queryAbsence = Pattern.<Event>begin("notA")
                .where(SimpleCondition.of(event -> !event.getActivity().equals("A")))
                .notFollowedBy("A")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .within(Time.seconds(3));

        // Query 6: Absence2(A)
        // Für den Fall A = 0, gilt Absence(A)

        Pattern<Event, ?> queryAbsence2 = Pattern.<Event>begin("start")
                .where(SimpleCondition.of(event -> !event.getActivity().equals("A")))
                .optional()
                .followedBy("A1")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .notFollowedBy("A2")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .within(Time.seconds(5));

        // Query 7: Absence3(A)
        // Für den Fall A = 0 gilt Absence(A)
        // Für den Fall A = 1 gilt Absence2(A)

        Pattern<Event, ?> queryAbsence3 = Pattern.<Event>begin("start")
                .where(SimpleCondition.of(event -> !event.getActivity().equals("A")))
                .optional()
                .followedBy("A1")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .followedBy("A2")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .notFollowedBy("A3")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .within(Time.seconds(5));

        // Query 8: Absence2Optional

        // Pattern<Event, ?> queryAbsenceOptional = Pattern.<Event>begin("start")
        // .where(SimpleCondition.of(event -> !event.getActivity().equals("A")))
        // .oneOrMore()
        // .optional()
        // .next("A1")
        // .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
        // .optional()
        // .next("notA")
        // .where(SimpleCondition.of(event -> !event.getActivity().equals("A")))
        // .oneOrMore()
        // .optional()
        // .until(SimpleCondition.of(event -> event.getActivity().equals("A")));

        // Query 9: Exactly1(A)

        Pattern<Event, ?> queryExactly1 = Pattern.<Event>begin("start")
                .where(SimpleCondition.of(event -> !event.getActivity().equals("A"))).oneOrMore()
                .optional()
                .next("A1")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .notFollowedBy("notA")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .within(Time.seconds(10));

        // Query 9: Exactly2(A)

        Pattern<Event, ?> queryExactly2 = Pattern.<Event>begin("start")
                .where(SimpleCondition.of(event -> !event.getActivity().equals("A"))).oneOrMore()
                .optional()
                .next("A1")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .followedBy("A2")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .notFollowedBy("A3")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .within(Time.seconds(10));

        // Query 10: Choice (A,B)

        Pattern<Event, ?> queryChoice = Pattern.<Event>begin("start")
                .where(SimpleCondition.of(event -> !event.getActivity().equals("A")))
                .where(SimpleCondition.of(event -> !event.getActivity().equals("B")))
                .oneOrMore()
                .optional()
                .next("choice")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .or(SimpleCondition.of(event -> event.getActivity().equals("B")))
                .next("end")
                .oneOrMore()
                .optional()
                .within(Time.seconds(10));

        // Query 11: ExclusiveChoice(A,B)

        Pattern<Event, ?> queryExclusiveChoice = Pattern.<Event>begin("exclusiveChoice")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .notFollowedBy("B")
                .where(SimpleCondition.of(event -> event.getActivity().equals("B")))
                .within(Time.seconds(10));

        // Query 12: ExclusiveChoice(A,B) Part 2

        Pattern<Event, ?> queryExclusiveChoicePart2 = Pattern.<Event>begin("exclusiveChoice")
                .where(SimpleCondition.of(event -> event.getActivity().equals("B")))
                .notFollowedBy("A")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .within(Time.seconds(10));

        // Query 13: RespondedExistence(A,B)
        // Wenn A nicht auftritt, dann ist RespondedExistence immer erfüllt, der Fall
        // wird durch Absence(A) abgedeckt
        // Es könnte aber auch zuerst B auftreten und danach erst A, für den Fall gilt
        // das Pattern Co-Existence(A,B)

        Pattern<Event, ?> queryRespondedExistence = Pattern.<Event>begin("A")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .followedBy("B")
                .where(SimpleCondition.of(event -> event.getActivity().equals("B")))
                .within(Time.seconds(10));

        // Query 14: Co-Existence(A,B)
        // Für den umgekehrten Fall gilt RespondedExistence(A,B)
        // Für B = 0 gilt Absence(B) -> werde ich hier nicht berücksichtigen

        Pattern<Event, ?> queryCoExistence = Pattern.<Event>begin("B")
                .where(SimpleCondition.of(event -> event.getActivity().equals("B")))
                .next("A")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .within(Time.seconds(10));

        // Query 15: Response(A,B)

        Pattern<Event, ?> queryResponse = Pattern.<Event>begin("A")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .followedBy("B")
                .where(SimpleCondition.of(event -> event.getActivity().equals("B")));

        // Query 16: Precedence(A, B)
        // Wenn B nicht auftritt, dann muss vorher auch kein A auftreten, dieser Fall
        // wäre mit Absence(B) zu vervollständigen, auf Grund der Übersichtlichkeit
        // verzichte ich darauf

        Pattern<Event, ?> queryPrecedence = Pattern.<Event>begin("noB")
                .where(SimpleCondition.of(event -> !event.getActivity().equals("B")))
                .oneOrMore()
                .optional()
                .next("A")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .next("end")
                .oneOrMore()
                .optional()
                .within(Time.seconds(10));

        // Query 17: Succession(A, B)

        Pattern<Event, ?> querySuccession = Pattern.begin(
                Pattern.<Event>begin("Start")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals("A")))
                        .where(SimpleCondition.of(event -> !event.getActivity().equals("B")))
                        .oneOrMore()
                        .optional()
                        .next("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                        .followedBy("B")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("B"))))
                .oneOrMore()
                .within(Time.seconds(10));

        // Query 18: AlternateResponse(A,B)

        Pattern<Event, ?> queryAlternateResponse = Pattern.begin(
                Pattern.<Event>begin("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                        .notFollowedBy("noA")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                        .followedBy("B")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("B"))))
                .oneOrMore()
                .within(Time.seconds(10));

        // Query 19: AlternatePrecedence(A,B)

        Pattern<Event, ?> queryAlternatePrecedence = Pattern.begin(
                Pattern.<Event>begin("noB")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals("B")))
                        .oneOrMore()
                        .optional()
                        .next("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                        .followedBy("B")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("B")))
                        .optional())
                .oneOrMore()
                .within(Time.seconds(10));

        // Query 20: AlternateSuccession(A,B)
        // Ist eigentlich eine Erweiterung von AlternatePrecedence, mit der Bedingung,
        // dass auf ein A ein B folgen muss

        Pattern<Event, ?> queryAlternateSuccession = Pattern.begin(
                Pattern.<Event>begin("noB")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals("B")))
                        .oneOrMore()
                        .optional()
                        .next("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                        .notFollowedBy("noA")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals("A")))
                        .followedBy("B")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("B"))))
                .oneOrMore()
                .within(Time.seconds(10));

        // Query 21: Chain Response(A,B)

        Pattern<Event, ?> queryChainResponse = Pattern.begin(
                Pattern.<Event>begin("noA")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals("A"))).oneOrMore().optional()
                        .next("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                        .next("B")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("B"))))
                .oneOrMore()
                .within(Time.seconds(10));

        // Query 22: ChainPrecedence(A,B)

        Pattern<Event, ?> queryChainPrecedence = Pattern.begin(
                Pattern.<Event>begin("noB")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals("B")))
                        .oneOrMore()
                        .optional()
                        .next("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                        .next("B")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("B")))
                        .optional())
                .oneOrMore()
                .within(Time.seconds(10));

        // Query 23: ChainSuccession(A,B)

        Pattern<Event, ?> queryChainSuccession = Pattern.begin(
                Pattern.<Event>begin("noB")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals("B")))
                        .where(SimpleCondition.of(event -> !event.getActivity().equals("A")))
                        .oneOrMore()
                        .optional()
                        .next("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                        .next("B")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("B"))))
                .oneOrMore()
                .within(Time.seconds(10));

        // Query 24: NotCo-Existence(A,B):
        // Der umgekehrte Fall wäre es gibt kein A, wenn ein B im Trace ist

        Pattern<Event, ?> queryNotCoExistence = Pattern.<Event>begin("noB")
                .where(SimpleCondition.of(event -> !event.getActivity().equals("B")))
                .oneOrMore()
                .optional()
                .next("A")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .notFollowedBy("B")
                .where(SimpleCondition.of(event -> event.getActivity().equals("B")))
                .within(Time.seconds(10));

        // Query 25: NotSuccession(A,B):
        // Für den Fall A = 0: Absence(A)
        Pattern<Event, ?> queryNotSuccession = Pattern.<Event>begin("noA")
                .where(SimpleCondition.of(event -> !event.getActivity().equals("A")))
                .oneOrMore()
                .optional()
                .next("A")
                .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                .notFollowedBy("B")
                .where(SimpleCondition.of(event -> event.getActivity().equals("B")))
                .within(Time.seconds(10));

        // Query 26: NotChainSuccession(A,B):
        // Für den Fall A = 0: Absence(A)

        Pattern<Event, ?> queryNotChainSuccession = Pattern.begin(
                Pattern.<Event>begin("noA")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals("A")))
                        .oneOrMore()
                        .optional()
                        .next("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("A")))
                        .notNext("B")
                        .where(SimpleCondition.of(event -> event.getActivity().equals("B"))))
                .oneOrMore()
                .within(Time.seconds(10));

        // Ergebnisse der Queries

        DataStream<String> result1 = CEP.pattern(inputEventStream, queryInit)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Init(A) Match: " + pattern.get("init");
                });

        DataStream<String> result2 = CEP.pattern(inputEventStream, queryExistence)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Existence(A) Match: " + pattern.get("existence");
                });

        DataStream<String> result3 = CEP.pattern(inputEventStream, queryExistence2)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Existence2(A) Match: " + pattern.get("A1");
                });
        DataStream<String> result4 = CEP.pattern(inputEventStream, queryExistence3)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Existence3(A) Match: " + pattern.get("A1");
                });
        DataStream<String> result5 = CEP.pattern(inputEventStream, queryAbsence)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Absence(A) Match: " + pattern.get("notA");
                });
        DataStream<String> result6 = CEP.pattern(inputEventStream, queryAbsence2)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Absence2(A) Match: " + pattern.get("A1");
                });
        DataStream<String> result7 = CEP.pattern(inputEventStream, queryAbsence3)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Absence3(A) Match: " + pattern.get("start");
                });
        DataStream<String> result8 = CEP.pattern(inputEventStream, queryExactly1)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Exactly1(A) Match: " + pattern.get("start");
                });
        DataStream<String> result9 = CEP.pattern(inputEventStream, queryExactly2)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Exactly2(A) Match: " + pattern.get("start");
                });
        DataStream<String> result10 = CEP.pattern(inputEventStream, queryChoice)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Choice(A,B) Match: " + pattern.get("start");
                });
        DataStream<String> result11 = CEP.pattern(inputEventStream, queryExclusiveChoice)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "ExclusiveChoice(A,B) Match: " + pattern.get("exclusiveChoice");
                });
        DataStream<String> result12 = CEP.pattern(inputEventStream, queryExclusiveChoicePart2)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "ExclusiveChoice2(A,B) Match: " + pattern.get("exclusiveChoice");
                });
        DataStream<String> result13 = CEP.pattern(inputEventStream, queryRespondedExistence)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "RespondedExistence(A,B) Match: " + pattern.get("A");
                });
        DataStream<String> result14 = CEP.pattern(inputEventStream, queryCoExistence)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Co-Existence(A,B) Match: " + pattern.get("B");
                });
        DataStream<String> result15 = CEP.pattern(inputEventStream, queryResponse)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Response(A,B) Match: " + pattern.get("A");
                });
        DataStream<String> result16 = CEP.pattern(inputEventStream, queryPrecedence)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Precedence(A,B) Match: " + pattern.get("noB");
                });
        DataStream<String> result17 = CEP.pattern(inputEventStream, querySuccession)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "Succession(A,B) Match: " + pattern.get("start");
                });
        DataStream<String> result18 = CEP.pattern(inputEventStream, queryAlternateResponse)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "AlternateResponse(A,B) Match: " + pattern.get("A");
                });
        DataStream<String> result19 = CEP.pattern(inputEventStream, queryAlternatePrecedence)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "AlternatePrecedence(A,B) Match: " + pattern.get("noB");
                });
        DataStream<String> result20 = CEP.pattern(inputEventStream, queryAlternateSuccession)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "AlternateSuccession(A,B) Match: " + pattern.get("noB");
                });
        DataStream<String> result21 = CEP.pattern(inputEventStream, queryChainResponse)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "ChainResponse(A,B) Match: " + pattern.get("noA");
                });
        DataStream<String> result22 = CEP.pattern(inputEventStream, queryChainPrecedence)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "ChainPrecedence(A,B) Match: " + pattern.get("noB");
                });
        DataStream<String> result23 = CEP.pattern(inputEventStream, queryChainSuccession)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "ChainSuccession(A,B) Match: " + pattern.get("noB");
                });
        DataStream<String> result24 = CEP.pattern(inputEventStream, queryNotCoExistence)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "NotCoExistence(A,B) Match: " + pattern.get("noB");
                });
        DataStream<String> result25 = CEP.pattern(inputEventStream, queryNotSuccession)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "NotSuccession(A,B) Match: " + pattern.get("noA");
                });
        DataStream<String> result26 = CEP.pattern(inputEventStream, queryNotChainSuccession)
                .select((PatternSelectFunction<Event, String>) pattern -> {
                    return "NotChainSucession(A,B) Match: " + pattern.get("noA");
                });

        // DataStream<String> result = CEP.pattern(inputEventStream,
        // queryAbsenceOptional)
        // .select((PatternSelectFunction<Event, String>) pattern -> {
        // // Extrahiere die gesamte Sequenz für "first" und "second"
        // StringBuilder matchOutput = new StringBuilder("Absence2(A) optional Match:
        // ");
        // pattern.forEach((state, events) -> {
        // matchOutput.append(state).append(": ").append(events).append("; ");
        // });
        // return matchOutput.toString();
        // });

        // Ergebnisse ausgeben
        // result1.print();
        // result2.print();
        // result3.print();
        // result4.print();
        // result5.print();
        // result6.print();
        // result7.print();
        // result8.print();
        // result9.print();
        // result10.print();
        // result11.print();
        // result12.print();
        // result13.print();
        // result14.print();
        // result15.print();
        // result16.print();
        // result17.print();
        // result18.print();
        // result19.print();
        // result20.print();
        // result21.print();
        // result22.print();
        // result23.print();
        // result24.print();
        // result25.print();
        // result26.print();

        env.execute("CEP monitoring job");
    }

}
