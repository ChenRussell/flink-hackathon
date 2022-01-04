package org.flink.hackathon;

import com.alibaba.fastjson.JSON;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.flink.hackathon.model.BussMsg;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.flink.hackathon.util.SqlUtil.*;

public class FlinkHackathonPravega2Mysql {

    public static Stream getStream(PravegaConfig pravegaConfig, String streamName) {
        return pravegaConfig.resolve(streamName);
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateTtlConfig.newBuilder(Time.minutes(2));
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(params.get("scope", "flink-hackathon"));

        Stream stream = getStream(
                pravegaConfig,
                params.get("stream", "business-data"));


        FlinkPravegaReader<String> source = FlinkPravegaReader.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withDeserializationSchema(new SimpleStringSchema())
                .build();

        DataStream<String> pravegaSource = env.addSource(source, "pravega source");
        DataStream<BussMsg> msgStream = pravegaSource.map(msg -> {
            BussMsg bussMsg = JSON.parseObject(msg, BussMsg.class);
            bussMsg.setCreateTimeStr(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(bussMsg.getCreateTime())));
            bussMsg.setAppointmentTimeStr(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(bussMsg.getAppointmentTime())));
            return bussMsg;
        });

        String tableName = "buss_pravega";
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

