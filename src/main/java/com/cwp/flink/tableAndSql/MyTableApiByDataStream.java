package com.cwp.flink.tableAndSql;

import com.cwp.flink.vo.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;


public class MyTableApiByDataStream {

    public static void main(String[] args) {
        StreamExecutionEnvironment streamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings  environmentSettings=EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment=StreamTableEnvironment.create(streamExecutionEnvironment,environmentSettings);


        DataStream<StationLog>  dataStream=streamExecutionEnvironment.socketTextStream("localhost",9000)
                .map(new MapFunction<String,StationLog>(){
                    @Override
                    public StationLog map(String value) throws Exception {
                        String[]  strArr=value.split(",");
                        return new StationLog(strArr[0],Integer.valueOf(strArr[1]),strArr[2],strArr[3],Long.valueOf(strArr[4]),strArr[5],
                                Long.valueOf(strArr[6]));
                    }
                });


        Table table=streamTableEnvironment.fromDataStream(dataStream);

        table.printSchema();








    }
}
