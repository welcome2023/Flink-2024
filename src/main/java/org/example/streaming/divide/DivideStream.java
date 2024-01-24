package org.example.streaming.divide;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @createDate 2023-12-29 20:22
 */

@Slf4j
public class DivideStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("10.173.28.35:9092,10.173.28.36:9092,10.173.28.37:9092")
                .setTopics("test1116")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        // 1.读kafka
        SingleOutputStreamOperator<JSONObject> readKafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").map(JSON::parseObject);

        // 2.分流
        Map<String, DataStream<JSONObject>> streams = splitStream(readKafkaStream);

        // 3.打印到控制台
        writeToKafka(streams);

        env.execute();
    }

    private static void writeToKafka(Map<String, DataStream<JSONObject>> streams) {
//      streams.get("info").map(JSONAware::toJSONString).print();
        streams.get("tags").map(JSONAware::toJSONString).print();
    }

    private static Map<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> stream) {
        // 主流不需要打标签
        OutputTag<JSONObject> tagsTag = new OutputTag<JSONObject>("tags", TypeInformation.of(JSONObject.class));
        // 输出ods数据
        SingleOutputStreamOperator<JSONObject> infoStream = stream
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        // 1.info数据
                        JSONArray infosBeams = obj.getJSONArray("info");
                        if (infosBeams != null) {
                            for (int i = 0; i < infosBeams.size(); i++) {
                                JSONObject infoBeam = infosBeams.getJSONObject(i);
                                out.collect(infoBeam);
                            }
//                            obj.remove("info");//移除 info 这个 key, 因为后面的数据处理用不上
                        }
                        // 2.tags数据
                        JSONObject tags = obj.getJSONObject("tags");
                        if (tags != null) {
                            ctx.output(tagsTag, tags);
//                            obj.remove("tags");
                        }
                    }});

        SideOutputDataStream<JSONObject> tagsStream = infoStream.getSideOutput(tagsTag);
        Map<String, DataStream<JSONObject>> streams = new HashMap<>();
        streams.put("info", infoStream);
        streams.put("tags", tagsStream);
        return streams;
    }

}
