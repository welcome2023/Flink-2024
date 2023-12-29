package org.example.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author zhanglingxing
 * @date 2023/11/7 11:09
 */
@Slf4j
public class FlinkUtils {

    public static ParameterTool setEnvParameters(StreamExecutionEnvironment env,String[] args) throws IOException {

        if (env instanceof LocalStreamEnvironment) {  // 在本地运行的逻辑
            env.enableCheckpointing(10000);
            env.setParallelism(1);
            log.info("flink提交作业模式：--本地");
        } else { // 在集群运行的逻辑
            //获取配置的参数 设置streampark参数的参数并封装
            log.info("flink提交作业模式：--集群");
        }
        ParameterTool parameter = ParameterTool.fromArgs(args);
        // 设置到了flink的环境中 可以在open方法中获取
        env.getConfig().setGlobalJobParameters(parameter);
        return  parameter;
    }


    public static <T> DataStreamSource<T> createKafkaDataStream(StreamExecutionEnvironment env,
                                                                String kafkaBootstrapServers, String topics, String groupId,String startingOffsets,
                                                                Class<? extends DeserializationSchema<T>> clazz) throws Exception {

        List<String> topicList = Arrays.asList(topics.split(","));
        // 初始化一个kafka souce
        KafkaSource<T> kafkaSource = KafkaSource.<T>builder()
                // 设置topic主题
                .setTopics(topicList)
                //设置组ID
                .setGroupId(groupId)
                .setBootstrapServers(kafkaBootstrapServers)
                // setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)) 消费起始位置选择之前提交的偏移量（如果没有，则重置为latest）
                // .setStartingOffsets(OffsetsInitializer.latest()) 最新的
                // .setStartingOffsets(OffsetsInitializer.earliest()) 最早的
                // .setStartingOffsets(OffsetsInitializer.offsets(Map< TopicPartition,Long >)) 之前具体的偏移量进行消费 每个分区对应的偏移量
                .setStartingOffsets(OffsetsInitializer.committedOffsets(getOffsetResetStrategy(startingOffsets)))
                .setValueOnlyDeserializer(clazz.newInstance())
                // 默认是关闭的
                // 开启了kafka的自动提交偏移量机制 会把偏移量提交到 kafka的 consumer_offsets中
                // 就算算子开启了自动提交偏移量机制，kafkaSource依然不依赖自动提交的偏移量（优先从flink自动管理的状态中获取对应的偏移量 如果获取不到就会用自动提交的偏移量）
                //将本source算子设置为 Bounded属性（有界流），将来改source去读取数据的时候，读到指定的位置，就停止读取并退出程序
                .setProperty("auto.offset.commit", "ture")
                .build();

        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");
    }

    private static OffsetResetStrategy getOffsetResetStrategy(String startingOffsets) {  //
        if ( "earliest-offset".equalsIgnoreCase(startingOffsets)) {
            return OffsetResetStrategy.EARLIEST;
        }
        // 默认使用 LATEST
        return OffsetResetStrategy.LATEST;
    }

    public static StreamExecutionEnvironment getEnv(ParameterTool parameters) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将ParameterTool的参数设置成全局的参数
        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(1);

        //开启checkpoint
        env.enableCheckpointing(parameters.getLong("checkpoint.interval", 1000L), CheckpointingMode.EXACTLY_ONCE);
        // Checkpoint 语义：EXACTLYONCE 或 ATLEASTONCE，EXACTLYONCE
        // EXACTLY_ONCE:表示所有要消费的数据被恰好处理一次，即所有数据既不丢数据也不重复消费；
        // AT_LEAST_ONCE: 表示要消费的数据至少处理一次，可能会重复消费
        // Checkpoint 语义设置为 EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // CheckPoint 的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 同一时间，只允许 有 1 个 Checkpoint 在发生
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 两次 Checkpoint 之间的最小时间间隔为 500 毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        //设置cancel任务不用删除checkpoint
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 重启策略 开启  task级别故障自动 failover
        // env.setRestartStrategy(RestartStrategies.noRestart()); // 默认是，不会自动failover；一个task故障了，整个job就失败了
        // 使用的重启策略是： 固定重启上限和重启时间间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(parameters.getInt("restart.times", 3), Time.seconds(10)));

        /**
         * 设置要使用的状态后端
         */
        HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
        env.setStateBackend(hashMapStateBackend); // 使用HashMapStateBackend  作为状态后端


        //设置statebackend
        String path = parameters.get("state.backend.path");
        if(path != null) {
            env.getCheckpointConfig().setCheckpointStorage("file:////Volumes/D/tmp/flink/ckpt");
        }

        return env;
    }


}
