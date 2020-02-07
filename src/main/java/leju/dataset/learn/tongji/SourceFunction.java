package leju.dataset.learn.tongji;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class SourceFunction implements ParallelSourceFunction<String> {

    boolean isRunning =true;
    String data ;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (true){
            data = buildString();
            ctx.collect(data);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public static String buildString(){
        StringBuilder builder = new StringBuilder();
        builder.append("leju").append("\t")
                .append(getLevel()).append("\t")
                .append(new SimpleDateFormat("yyy-MM-dd HH:mm:ss").format(new Date())).append("\t")
                .append(getIps()).append("\t")
                .append(getDomain()).append("\t")
                .append(getTraffic()).append("\t")
        ;

        return builder.toString();
    }

    private static long getTraffic() {
        return new Random().nextInt(10000);
    }

    private static String getDomain() {
        String[] Domain = new String[]{
                "www.leju.com",
                "house.leju.com",
                "m.leju.com",
                "mf.leju.com",
                "tongji.leju.com",
                "nk.leju.com",
                "bj.leju.com",
        };

        return Domain[new Random().nextInt(Domain.length)];
    }

    private static String getIps() {

        String[] ips = new String[]{
                "223.104.18.110",
                "123.14.18.110",
                "23.10.8.10",
                "22.104.18.120",
                "223.1.18.223",
                "2.104.18.1",
                "3.14.18.120",
                "23.144.28.30",
                "23.134.38.150",
                "10.134.48.110",
                "13.124.58.120",
        };

        return ips[new Random().nextInt(ips.length)];
    }

    //生成level数据
    public static String getLevel(){
        String[] levels = new String[]{"E","M"};
        return  levels[new Random().nextInt(levels.length)];
    }
}
