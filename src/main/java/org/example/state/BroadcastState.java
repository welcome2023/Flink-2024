package org.example.state;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;
import org.example.function.WaterSensorMapFunction;

/**
 * @author cmsxyz@163.com
 * @date 2024-01-25 23:36
 */
public class BroadcastState {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //准备数据流
        SingleOutputStreamOperator<WaterSensor> dataDs = env
                .socketTextStream("cdh01", 8888)
                .map(new WaterSensorMapFunction());
        //准备配置流
        SingleOutputStreamOperator<MyConf> configDs = env
                .socketTextStream("cdh02", 8889)
                .map(new MapFunction<String, MyConf>() {
                    @Override
                    public MyConf map(String value) throws Exception {
                        String[] words = value.split(",");
                        return new MyConf(words[0], words[1]);
                    }
                });

        //只有广播流，才能使用广播状态。 把配置流转换为广播流
        MapStateDescriptor<String, MyConf> confMapStateDescriptor = new MapStateDescriptor<>("config", String.class, MyConf.class);
        BroadcastStream<MyConf> configBroadcastDS = configDs.broadcast(confMapStateDescriptor);
        //两个流需要connect，否则无法广播
        dataDs.connect(configBroadcastDS)
                .process(new BroadcastProcessFunction<WaterSensor, MyConf, String>() {
                    //处理数据流
                    @Override
                    public void processElement(WaterSensor value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        //获取广播状态，才能取出里面的配置
                        ReadOnlyBroadcastState<String, MyConf> broadcastState = ctx.getBroadcastState(confMapStateDescriptor);
                        MyConf myConf = broadcastState.get(value.getId());
                        value.setId(myConf.value);
                        out.collect(value.toString());
                    }

                    //配置配置流
                    @Override
                    public void processBroadcastElement(MyConf value, Context ctx, Collector<String> out) throws Exception {

                        //一旦配置更新了，就把新的配置数据存入广播状态
                        org.apache.flink.api.common.state.BroadcastState<String, MyConf> broadcastState = ctx.getBroadcastState(confMapStateDescriptor);
                        //当Map用,只要存进入可以自动广播到数据流的所有的Task
                        broadcastState.put(value.name, value);

                    }
                })
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MyConf {
        private String name;
        private String value;
    }
}
