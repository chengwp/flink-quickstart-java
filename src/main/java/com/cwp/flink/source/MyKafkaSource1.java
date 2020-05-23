package com.cwp.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class MyKafkaSource1 {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","192.168.10.198:9092");
        properties.setProperty("group.id","flink01");
        properties.setProperty("key.deserializer",StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("auto.offset.reset","latest");

        DataStream<String>  dataStream=streamEnv.addSource(new FlinkKafkaConsumer010<String>("cxh",new SimpleStringSchema(),properties));
        dataStream.print();
        streamEnv.execute("MyKafkaSource1");

    }
}
