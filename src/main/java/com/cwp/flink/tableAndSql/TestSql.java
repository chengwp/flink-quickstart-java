package com.cwp.flink.tableAndSql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;


public class TestSql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings  environmentSettings=EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment=StreamTableEnvironment.create(streamExecutionEnvironment,environmentSettings);

        streamExecutionEnvironment.setParallelism(1);

        String[]  fieldNames={"no","sid","call_in","times","call_out","flag","duration"};
        TableSource tableSource=new CsvTableSource("/Users/chengweiping/develop/gitProjects/flink-quickstart-java/src/main/resource/station.txt",fieldNames,
                new TypeInformation[]{Types.INT,Types.STRING,Types.STRING,Types.INT,Types.STRING,Types.STRING,Types.LONG});

        streamTableEnvironment.registerTableSource("t_station",tableSource);

        Table table=streamTableEnvironment.sqlQuery(" select  sid,sum(duration)  from  t_station where flag='success' group by sid");

        streamTableEnvironment.toRetractStream(table,Row.class).filter(e->e.f0==true).print();

        streamTableEnvironment.execute("sql test");


    }
}
