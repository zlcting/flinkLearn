package leju.dataset.learn.tongji;
import leju.dataset.learn.course05.JavaCustomParallelSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JavaDataStreamSourceApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //socketFunction(env);
        nonParallelSourceFunction(env);
        //ParallelSourceFunction(env);
        //richParallelSourceFunction(env);
        env.execute("JavaDataStreamSourceApp");

    }

    public static void richParallelSourceFunction(StreamExecutionEnvironment env){
        DataStreamSource<Long> data = env.addSource(new JavaCustomParallelSourceFunction()).setParallelism(2);
        data.print().setParallelism(1);
    }


    public static void ParallelSourceFunction(StreamExecutionEnvironment env){
        DataStreamSource<Long> data = env.addSource(new JavaCustomParallelSourceFunction()).setParallelism(2);
        data.print().setParallelism(1);
    }



    public static void nonParallelSourceFunction(StreamExecutionEnvironment env){
        DataStreamSource<String> data = env.addSource(new SourceFunction());
        data.print().setParallelism(1);
    }


    public static void socketFunction(StreamExecutionEnvironment env){

        DataStreamSource<String> data = env.socketTextStream("127.0.0.1",9001);
        data.print().setParallelism(1);
    }
}
