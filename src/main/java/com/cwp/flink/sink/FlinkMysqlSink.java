package com.cwp.flink.sink;

import com.cwp.flink.source.MyCustomeSource;
import com.cwp.flink.vo.StationLog;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class FlinkMysqlSink extends RichSinkFunction<StationLog> {

    Connection conn=null;
    PreparedStatement pst=null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String url="jdbc:mysql://localhost:3306/flink";
        String username="root";
        String password = "123456";
        conn=DriverManager.getConnection(url,username,password);
        String sql= " insert into station(call_index,call_in,call_out,call_time,call_type) values(?,?,?,?,?)";
        pst=conn.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        conn.close();
        pst.close();
    }

    @Override
    public void invoke(StationLog value, Context context) throws Exception {
        pst.setInt(1,value.getIndex());
        pst.setString(2,value.getCallIn());
        pst.setString(3,value.getCallOut());
        pst.setLong(4, value.getTime());
        pst.setString(5,value.getType());
        pst.executeUpdate();
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        DataStream<StationLog> dataStream=streamEnv.addSource(new MyCustomeSource());
        dataStream.addSink(new FlinkMysqlSink());

        try {
            streamEnv.execute("MyCustomeSource");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



