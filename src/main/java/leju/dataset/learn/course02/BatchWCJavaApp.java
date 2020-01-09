package leju.dataset.learn.course02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchWCJavaApp {
    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String input = "file:///home/zlc/project/flink_project/learn/input";

        DataSource<String> text = env.readTextFile(input);
        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
              String[] tokens =  value.toLowerCase().split("/t");
              for(String token :tokens){
                  collector.collect(new Tuple2<>(token,1));
              }
            }
        }).groupBy(0).sum(1).print();
    }
}
