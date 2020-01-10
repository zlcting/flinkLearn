package leju.dataset.learn.course05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JavaCustomSinkToMysql {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1",9001,"\n");
       SingleOutputStreamOperator<Student> studentStream =  source.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] splits = value.split(",");
                Student stu = new Student();
                stu.setId(Integer.parseInt(splits[0]));
                stu.setName(splits[1]);
                stu.setAge(Integer.parseInt(splits[2]));
                return stu;
            }
        });
        studentStream.addSink(new SinkToMysql());
        env.execute("JavaCustomSinkToMysql");
    }
}
