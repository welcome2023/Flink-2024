package org.example.streaming.sink;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.example.bean.WaterSensor;

import java.time.Duration;

/**
 * @author cmsxyz@163.com
 * @date 2024/1/25 18:01
 * @usage
 */
public class FileSinkTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 新的flieSink需要开启Checkpoint，才会生效,每5s备份一次
        env.enableCheckpointing(5000);
        env.setParallelism(2);
        /*
                导入 FileConnector
                    forRowFormat(): 写行式存储的文件
                    withRollingPolicy(): 指定文件的滚动策略。
                                            按照文件大小滚动
                                            按照时间间隔滚动
                                          DefaultRollingPolicy的默认参数:
                                            1.按照文件大小滚动，默认 128M
                                            2.按照时间间隔滚动，距离上次滚动超过60s，就再次滚动
                                            3.超过60s没有写入新的内容，就滚动
                    withBucketAssigner(): 文件如何分桶(目录)
                    withOutputFileConfig(): 指定输出的文件的前缀和后缀
                    withBucketCheckInterval(): 检测文件滚动的时间是否合法，和withRollingPolicy中的某个滚动策略对应。
         */
        FileSink<String> fileSink = FileSink
                .<String>forRowFormat(new Path("D:\\001_Job\\001_ProjectCode\\Flink-2024\\src\\file"), new SimpleStringEncoder<>())
                .withRollingPolicy(
                        //文件达到1m就滚动 或 每间隔30s滚动 或 超过50s没有写入新的数据就滚动
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(MemorySize.parse("1m")) //设置基于大小滚动的策略
                                .withRolloverInterval(Duration.ofSeconds(5L))  //按照时间间隔滚动
                                .withInactivityInterval(Duration.ofSeconds(50L)) //超过多久没写入就滚动
                                .build()
                )
                //按照分钟滚动文件所存放的目录
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH-mm"))
                .withOutputFileConfig(new OutputFileConfig("cms-", ".log"))
                //和滚动策略中 withRolloverInterval(x)的x一致，基于时间间隔的滚动才会生效
                .withBucketCheckInterval(10000)
                .build();

        DataGeneratorSource<WaterSensor> dataGeneratorSource = new DataGeneratorSource<WaterSensor>(new RandomGenerator<WaterSensor>() {
            @Override
            public WaterSensor next() {
                return new WaterSensor(
                        "s" + RandomUtils.nextInt(1, 11),
                        System.currentTimeMillis(),
                        RandomUtils.nextInt(1000, 3001)
                );
            }
        },
                100,
                null);
        SingleOutputStreamOperator<String> ds = env.addSource(dataGeneratorSource).returns(WaterSensor.class)
                .map(JSON::toJSONString);

        // old api: 1.14之前的版本，不会标注为过时。env.addSink(SinkFunction x)
        // new api: 1.14开始推荐使用,env.sinkTo(Sink x)
        ds.sinkTo(fileSink);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
