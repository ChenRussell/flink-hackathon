package org.flink.hackathon.util;

/**
 * @project flink-hackathon
 * @date 2022/01/04
 */
public class SqlUtil {

    public static String getBussSinkTableSql2() {
        return "CREATE TABLE buss_mysql (\n" +
                "  customId INT,\n" +
                "  institutionId INT,\n" +
                "  businessType INT,\n" +
                "  businessAmount float,\n" +
                "  createTime BIGINT,\n" +
                "  appointmentTime BIGINT,\n" +
                "  managerId INT,\n" +
                "  company INT,\n" +
                "  hiValueClient INT,\n" +
                " PRIMARY KEY (customId) NOT ENFORCED " +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://47.104.176.73:3306/bank?charset=utf8',\n" +
                "   'username' = 'superset',\n" +
                "   'password' = '123456',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'table-name' = 'BUSS_cr'\n" +
                ")";
    }

    public static String getBussSinkTableSql() {
        return "CREATE TABLE buss_mysql (\n" +
                "  CUST_ID INT,\n" +
                "  INST_ID INT,\n" +
                "  LV1NM STRING,\n" +
                "  LV2NM STRING,\n" +
                "  LV3NM STRING,\n" +
                "  BUSS_TYPE STRING,\n" +
                "  BUSS_AMT float,\n" +
                "  CRT_TIME STRING,\n" +
                "  APPOINTMENT_TIME STRING,\n" +
                "  MANAGER_ID INT,\n" +
                "  MANAGER_NM STRING,\n" +
                "  COMPANY STRING,\n" +
                "  HIValue_CLIENT STRING,\n" +
                " PRIMARY KEY (CUST_ID) NOT ENFORCED " +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://47.104.176.73:3306/bank?charset=utf8',\n" +
                "   'username' = 'superset',\n" +
                "   'password' = '123456',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'table-name' = 'S_BUSS'\n" +
                ")";
    }

    public static String getManagerTableSql() {
        return "CREATE TABLE d_manager (\n" +
                "  MANAGER_ID INT,\n" +
                "  MANAGER_NM STRING,\n" +
                " PRIMARY KEY (MANAGER_ID) NOT ENFORCED "+
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://47.104.176.73:3306/bank?charset=utf8',\n" +
                "   'username' = 'superset',\n" +
                "   'password' = '123456',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'lookup.cache.ttl' = '6s',\n" +
                "   'sink.buffer-flush.interval' = '2s',\n" +
                "   'lookup.cache.max-rows' = '100',\n" +
                "   'table-name' = 'D_MANAGER'\n" +
                ")";
    }

    public static String getInsTableSql() {
        return "CREATE TABLE d_ins (\n" +
                "  INS_ID INT,\n" +
                "  LV1NM STRING,\n" +
                "  LV2NM STRING,\n" +
                "  LV3NM STRING,\n" +
                " PRIMARY KEY (INS_ID) NOT ENFORCED "+
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://47.104.176.73:3306/bank?charset=utf8',\n" +
                "   'username' = 'superset',\n" +
                "   'password' = '123456',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'lookup.cache.ttl' = '6s',\n" +
                "   'sink.buffer-flush.interval' = '2s',\n" +
                "   'lookup.cache.max-rows' = '100',\n" +
                "   'table-name' = 'D_INS'\n" +
                ")";
    }

    public static String getHiValueTableSql() {
        return "CREATE TABLE hi_value_cli (\n" +
                "  CUST_ID INT,\n" +
                " PRIMARY KEY (CUST_ID) NOT ENFORCED " +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://47.104.176.73:3306/bank?charset=utf8',\n" +
                "   'username' = 'superset',\n" +
                "   'password' = '123456',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'lookup.cache.ttl' = '6s',\n" +
                "   'sink.buffer-flush.interval' = '2s',\n" +
                "   'lookup.cache.max-rows' = '200',\n" +
                "   'table-name' = 'D_HIVALUE'\n" +
                ")";
    }

    public static String getSinkSql2() {
        return "insert into buss_mysql" +
                " select a.customId, a.institutionId, a.businessType, a.businessAmount, a.createTime, a.appointmentTime, a.managerId, a.company," +
                " case when b.CUST_ID is null then 0 else 1 end" +
                " from buss_pravega a" +
                " left join hi_value_cli b on a.customId=b.CUST_ID" +
                " left join d_ins c on a.institutionId=c.INS_ID" +
                " where a.businessType=3";
    }

    public static String getSinkSql(String tableName) {
        return "insert into buss_mysql" +
                " select a.customId, a.institutionId," +
                " c.LV1NM,c.LV2NM,c.LV3NM," +
                " '预约取款', a.businessAmount, " +
                " a.createTimeStr," +
                " a.appointmentTimeStr, " +
                " a.managerId,d.MANAGER_NM," +
                " case when a.company=0 then '对公业务' else '对私业务' end," +
                " case when b.CUST_ID is null then '低价值客户' else '高价值客户' end" +
                String.format(" from (select * from %s where businessType=3) a", tableName) +
                " left join hi_value_cli b on a.customId=b.CUST_ID" +
                " left join d_ins c on a.institutionId=c.INS_ID" +
                " left join d_manager d on a.managerId=d.MANAGER_ID";
    }
}
