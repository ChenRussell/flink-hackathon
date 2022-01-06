package org.flink.hackathon;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.flink.hackathon.model.BussMsg;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.flink.hackathon.util.SqlUtil.*;
import static org.flink.hackathon.util.SqlUtil.getBussSinkTableSql;

public class FlinkHackathonKafka2MysqlV2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateTtlConfig.newBuilder(Time.minutes(2));
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("***")
                .setTopics("flink-hackathon-business-data")
                .setGroupId("hackathon-20220104-flink")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<BussMsg> msgStream = kafkaSource.map(msg -> {
            BussMsg bussMsg = JSON.parseObject(msg, BussMsg.class);
            bussMsg.setCreateTimeStr(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(bussMsg.getCreateTime())));
            bussMsg.setAppointmentTimeStr(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(bussMsg.getAppointmentTime())));
            return bussMsg;
        });
        msgStream.print();

        String tableName = "buss_kafka";
        tableEnv.createTemporaryView(tableName, msgStream);

        String hiValueTableSql = getHiValueTableSql();

        String insTableSql = getInsTableSql();

        String managerTableSql = getManagerTableSql();

        String bussSinkTableSql = getBussSinkTableSql();


        String sinkSql = getSinkSql(tableName);


        //tableEnv.executeSql(bussTableSql);
        tableEnv.executeSql(hiValueTableSql);
        tableEnv.executeSql(insTableSql);
        tableEnv.executeSql(managerTableSql);
        tableEnv.executeSql(bussSinkTableSql);
        tableEnv.executeSql(sinkSql);
    }
}

