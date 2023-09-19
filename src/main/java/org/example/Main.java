package org.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("group")
                .setTopics("log")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new EventDeserializationSchema())
                .build();

        DataStream<Event> dataStream = executionEnvironment.fromSource(
                source,
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTs();
                            }
                        }),
                "Logs"
        );

        SingleOutputStreamOperator<Event> process = dataStream.keyBy(new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event event) throws Exception {
                        return event.getConf();
                    }
                })
                .process(new KeyedProcessFunction<String, Event, Event>() {

                    private ListState<Event> bufferState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<Event> bufferStateDescriptor = new ListStateDescriptor<>(
                                "bufferState",
                                Event.class
                        );
                        bufferState = getRuntimeContext().getListState(bufferStateDescriptor);

                    }

                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                        bufferState.add(value);

                        List<Event> buffer = new LinkedList<>();
                        for(Event event: bufferState.get()){
                            buffer.add(event);
                        }

                        buffer.sort(new Comparator<Event>() {
                            @Override
                            public int compare(Event o1, Event o2) {
                                long l = o1.getTs() - o2.getTs();

                                if(l > 0) {
                                    return 1;
                                } else if (l < 0) {
                                    return -1;
                                } else {
                                    return 0;
                                }
                            }
                        });

                        for (Event event: buffer){
                            out.collect(event);
                        }
                    }

                });

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return Objects.equals(event.getId(), "one");
                    }
                }).followedBy("middle")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return Objects.equals(event.getId(), "three");
                    }
                }).next("end")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return Objects.equals(event.getId(), "four");
                    }
                });

        PatternStream<Event> patternStream = CEP.pattern(dataStream, pattern, new EventComparator<Event>() {
                    @Override
                    public int compare(Event o1, Event o2) {
                        return (int) (o1.getTs() - o2.getTs());
                    }
                })
                .inEventTime();

        patternStream.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> map, Context context, Collector<String> collector) throws Exception {
                System.out.println(map);
                collector.collect("match");
            }
        });

        executionEnvironment.execute("CEP");

    }
}