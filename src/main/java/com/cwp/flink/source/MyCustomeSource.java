package com.cwp.flink.source;

import com.cwp.flink.vo.StationLog;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import scala.util.Random;

public class MyCustomeSource extends RichSourceFunction<StationLog> {

    boolean flag=true;

    @Override
    public void run(SourceContext<StationLog> sourceContext) throws Exception {

        Random r=new Random();
        while(flag){
            for (int i = 1; i <= 10; i++) {
                StationLog stationLog = new StationLog();
                stationLog.setIndex(i);
                stationLog.setCallOut(String.format("158899636%s",r.nextInt(10)));
                stationLog.setCallIn(String.format("1342098347%s",r.nextInt(10)));
                stationLog.setTime(System.currentTimeMillis());
                stationLog.setType("successs");
                sourceContext.collect(stationLog);
            }
            Thread.sleep(10000);
        }

    }

    @Override
    public void cancel() {

    }


    public static void main(String[] args) {
        StreamExecutionEnvironment  streamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        DataStream<StationLog> dataStream=streamEnv.addSource(new MyCustomeSource());

        dataStream.print();

        try {
            streamEnv.execute("MyCustomeSource");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
