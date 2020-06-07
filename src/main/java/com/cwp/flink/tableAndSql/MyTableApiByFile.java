package com.cwp.flink.tableAndSql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;


public class MyTableApiByFile {

    public static void main(String[] args) {
        StreamExecutionEnvironment streamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings  environmentSettings=EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment=StreamTableEnvironment.create(streamExecutionEnvironment,environmentSettings);


        String[]  fieldNames={"f1","f2","f3","f4","f5","f6","f7"};
        TableSource tableSource=new CsvTableSource("/station.txt",fieldNames,
                new TypeInformation[]{Types.INT,Types.STRING,Types.STRING,Types.INT,Types.STRING,Types.STRING,Types.LONG});

        Table table=streamTableEnvironment.fromTableSource(tableSource);

        table.printSchema();


    }
}
