package leju.dataset.learn.course04;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class JavaCount {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("haoop","linux","flink","php","golang","python");

        DataSet<String> info =  data.map(new RichMapFunction<String, String>() {
            //定义一个累加器
            LongCounter counter = new LongCounter();
            //
            public void open(Configuration parameters)throws Exception{
                super.open(parameters);
                //注册一个累加器
                getRuntimeContext().addAccumulator("ele-counts-java",counter);

            }
            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        });

        String filePaht = "/home/zlc/project/flink_project/learn/input/sink_out";
        info.writeAsText(filePaht, FileSystem.WriteMode.OVERWRITE).setParallelism(2);
        JobExecutionResult jobResult = env.execute("counterApp");
        //3.获取计数器
        Long num = jobResult.getAccumulatorResult("ele-counts-java");
        System.out.println("num["+num+"]");
    }
}
