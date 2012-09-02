package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.lang.Thread;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;

public class GrepTopology {
    public static class GrepBolt extends BaseRichBolt {
        private static String pattern = "light";
//        private static String pattern = "troop";
        private AtomicInteger count = new AtomicInteger(0);
        private long firstTime = 0;
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            long time = System.currentTimeMillis();
            int cnt = 0;
            for (String s: tuple.getString(0).split("\n")) {
                if (s.contains(pattern)) {
                    cnt = count.addAndGet(1);
                    if (cnt == 1) {
                        firstTime = time;
                    } else if (cnt == 10) {
                        _collector.emit(tuple, new Values(count.getAndSet(0), firstTime));
                    }
                }
            }
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("grep_count", "time"));
        }
    }

    public static class GrepResultBolt extends BaseRichBolt {
        OutputCollector _collector;
        private AtomicInteger count = new AtomicInteger(0);
        private int limit = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            int cnt = count.addAndGet(tuple.getInteger(0));
            if (cnt >= limit) {
                limit += 1000;
                long currentTime = System.currentTimeMillis();
                long time = tuple.getLong(1);
                System.out.println("Data Emited @ Time: " + time + " ms. Got Result Time: " + currentTime + " ms. Latency: " + (currentTime - time) + " ms. We got " + count.get() + " tuples");
            }
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("result"));
        }
    }

//    public static int BOOK_PER_SEC = 100;

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        if(args!=null && args.length > 0) {
            int nodes = 10;
            int bookPerSec = Integer.parseInt(args[1]);
            int blocks = Integer.parseInt(args[2]);

//            builder.setSpout("SenSpout", new SentenceSource(), nodes);
            builder.setSpout("SenSpout", new BookSource(bookPerSec, blocks), nodes);
            builder.setBolt("GrepBolt", new GrepBolt(), nodes * 2)
                    .localOrShuffleGrouping("SenSpout");
            builder.setBolt("ResultBolt", new GrepResultBolt(), 1)
                    .localOrShuffleGrouping("GrepBolt");
                
            Config conf = new Config();
//            conf.setDebug(true);

            conf.setNumWorkers(nodes * 4);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            int nodes = 1;

            builder.setSpout("SenSpout", new SentenceSource(), nodes);
//            builder.setSpout("SenSpout", new BookSource(), nodes);
            builder.setBolt("GrepBolt", new GrepBolt(), nodes)
                    .localOrShuffleGrouping("SenSpout");
            builder.setBolt("ResultBolt", new GrepResultBolt(), 1)
                    .localOrShuffleGrouping("GrepBolt");
                
            Config conf = new Config();
            conf.setDebug(true);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("GrepTopology", conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology("GrepTopology");
            cluster.shutdown();    
        }
    }
}
