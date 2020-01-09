package leju.dataset.learn.course04;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


import java.util.ArrayList;
import java.util.List;

//transformation 算子demo

public class JavaMapApp {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //MapFunction(env);
        //filterFunction(env);
        //firstFunction(env);
        flagMapFunction(env);
    }

    public static void flagMapFunction(ExecutionEnvironment env) throws Exception{
        List<String> info = new ArrayList<String>();
        info.add("Hadoop,spark");
        info.add("Hadoop,spark");
        info.add("linux,spark");
        info.add("php,win");
        info.add("yaf,spark");
        info.add("golang,vue");
        DataSource<String> data = env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> collector) throws Exception {
                String[] splits = input.split(",");
                for (String split :splits){
                    collector.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        }).groupBy(0).sum(1).print();

    }

    public static void firstFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer,String>> info = new ArrayList<Tuple2<Integer, String>>();
        info.add(new Tuple2<Integer, String>(1,"Hadoop"));
        info.add(new Tuple2<Integer, String>(1,"Spark"));
        info.add(new Tuple2<Integer, String>(1,"Flink"));
        info.add(new Tuple2<Integer, String>(2,"Java"));
        info.add(new Tuple2<Integer, String>(2,"PHP"));
        info.add(new Tuple2<Integer, String>(3,"Linux"));
        info.add(new Tuple2<Integer, String>(4,"yaf"));

        DataSource<Tuple2<Integer,String>> data = env.fromCollection(info);

        data.first(3).print();
        System.out.println("~~~~~~~~~~分割线~~~~~~~~~~~~~~~~");

        data.groupBy(0).first(2).print();
        System.out.println("~~~~~~~~~~分割线~~~~~~~~~~~~~~~~");

        data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
        System.out.println("~~~~~~~~~~分割线~~~~~~~~~~~~~~~~");
    }


    //filter 数据过滤
    public static void filterFunction(ExecutionEnvironment env) throws Exception{
        List<Integer> list = new ArrayList<Integer>();
        for (int i=1;i<=10;i++){
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);
        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer input) throws Exception {
                return input+1;
            }
        }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer input) throws Exception {

                return input>5;
            }
        }).print();
    }

    //map
    public static void MapFunction(ExecutionEnvironment env) throws Exception{
        List<Integer> list = new ArrayList<Integer>();
        for (int i=1;i<=10;i++){
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);

        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer input) throws Exception {
                return input+1;
            }
        }).print();
    }


}
