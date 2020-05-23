package com.cwp.flink.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

public class FlinkRedisSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStream<String> dataStream=streamEnv.socketTextStream("192.168.10.198",9000);
        DataStream resultDataStream=dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for(String str:s.split(" ")){
                   collector.collect(str);
              }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s,1);
            }
        }).keyBy(0).sum(1);


        FlinkJedisPoolConfig flinkJedisPoolConfig=new FlinkJedisPoolConfig.Builder().setHost("192.168.10.198").setPort(6379).setDatabase(3).build();


        resultDataStream.addSink(new RedisSink(flinkJedisPoolConfig, new RedisMapper<Tuple2<String,Integer>>() {

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"cwp");
            }

            @Override
            public String getKeyFromData(Tuple2<String, Integer> stringIntegerTuple2) {
                return stringIntegerTuple2.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, Integer> stringIntegerTuple2) {
                return stringIntegerTuple2.f1+"";
            }
        }));



        streamEnv.execute();
        
    }
    
    
}
