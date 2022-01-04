package org.flink.hackathon;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkHackathonKafka2Mysql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateTtlConfig.newBuilder(Time.minutes(2));
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bbTableEnv = StreamTableEnvironment.create(env, bsSettings);
        String bussTableSql= "CREATE TABLE buss_kafka (\n" +
                "  customId INT,\n" +
                "  institutionId INT,\n" +
                "  businessType INT,\n" +
                "  businessAmount float,\n" +
                "  createTime BIGINT,\n" +
                "  appointmentTime BIGINT,\n" +
                "  managerId INT,\n" +
                "  company INT\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'flink-hackathon-business-data',\n" +
                "  'properties.bootstrap.servers' = '47.104.176.73:9092',\n" +
                "  'properties.group.id' = 'hackathon-20220103-flink222',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'latest-offset'\n" +
//                "  'value.json.ignore-parse-errors' = 'true'\n" +
                ")";

        String hiValueTableSql="CREATE TABLE hi_value_cli (\n" +
                "  CUST_ID INT,\n" +
                " PRIMARY KEY (CUST_ID) NOT ENFORCED "+
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://47.104.176.73:3306/bank?charset=utf8',\n" +
                "   'username' = 'superset',\n" +
                "   'password' = '123456',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'lookup.cache.ttl' = '6s',\n" +
                "   'sink.buffer-flush.interval' = '2s',\n" +
                "   'lookup.cache.max-rows' = '1',\n" +
                "   'table-name' = 'D_HIVALUE'\n" +
                ")";

        String bussSinkTableSql= "CREATE TABLE buss_mysql (\n" +
                "  customId INT,\n" +
                "  institutionId INT,\n" +
                "  businessType INT,\n" +
                "  businessAmount float,\n" +
                "  createTime BIGINT,\n" +
                "  appointmentTime BIGINT,\n" +
                "  managerId INT,\n" +
                "  company INT,\n" +
                "  hiValueClient INT,\n" +
                " PRIMARY KEY (customId) NOT ENFORCED "+
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://47.104.176.73:3306/bank?charset=utf8',\n" +
                "   'username' = 'superset',\n" +
                "   'password' = '123456',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'table-name' = 'BUSS_cr'\n" +
                ")";


        String sinkSql = "insert into buss_mysql" +
                " select a.*," +
                " case when b.CUST_ID is null then 0 else 1 end" +
                " from buss_kafka a" +
                " left join hi_value_cli b on a.customId=b.CUST_ID" +
                " where a.businessType=3";


        bbTableEnv.executeSql(bussTableSql);
        bbTableEnv.executeSql(hiValueTableSql);
        bbTableEnv.executeSql(bussSinkTableSql);
        bbTableEnv.executeSql(sinkSql);
    }
}

