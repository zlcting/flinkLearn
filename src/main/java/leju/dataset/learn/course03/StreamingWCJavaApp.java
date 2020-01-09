package leju.dataset.learn.course03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamingWCJavaApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text =  env.socketTextStream("127.0.0.1",9001);

        text.flatMap(new MyFlatMapFunction())
                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .sum("count")
                .print();

        env.execute("zhixing");
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, WC> {
        @Override
        public void flatMap(String value, Collector<WC> collector) throws Exception {
            String[] tokens = value.toLowerCase().split("/t");
            for (String token:tokens){
                if (token.length()>0){
                    collector.collect(new WC(token,1));
                }
            }
        }
    }

    public static class WC{
        private String word;
        private int count;
        public WC(){}
        public WC(String word,int count){
            this.word=word;
            this.count=count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}
