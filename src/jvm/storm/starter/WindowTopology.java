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
import java.util.HashMap;
import java.util.LinkedList;

/**
 * This is a basic example of a Storm topology.
 */
public class WindowTopology {
    public static class Ele {
        public long currentSec;
        public long lastUpdateMs;
        public HashMap<String, Integer> counts;

        public Ele(long tCurrentSec) {
            currentSec = tCurrentSec;
            lastUpdateMs = -1;
            counts = new HashMap<String, Integer>();
        }

        public void addWord(String word, Integer cnt, long timeMs) {
            int tCnt = cnt;
            if (counts.containsKey(word)) {
                tCnt += counts.get(word);
            }
            counts.put(word, tCnt);
            lastUpdateMs = System.currentTimeMillis();
        }
    }

    public static class TopKEle {
        public long currentSec;
        public long lastUpdateMs;
        private int TopK;
        private int minCount = Integer.MAX_VALUE;
        private String minWord = null;
        public HashMap<String, Integer> counts;

        public TopKEle(long tCurrentSec, int topK) {
            currentSec = tCurrentSec;
            lastUpdateMs = -1;
            TopK = topK;
            counts = new HashMap<String, Integer>(TopK);
        }

        public void addWord(String word, Integer cnt) {
            if (counts.size() < TopK) {
                counts.put(word, cnt);
                if (cnt < minCount) {
                    minCount = cnt;
                    minWord = word;
                }
                return;
            }

            if (cnt <= minCount) {
                return;
            }
            counts.remove(minWord);
            counts.put(word, cnt);
            minWord = word;
            minCount = cnt;
            for (Map.Entry<String, Integer> entry: counts.entrySet()) {
                if (entry.getValue() < minCount) {
                    minCount = entry.getValue();
                    minWord = entry.getKey();
                }
            }
        }
    }

    public static class GrepBolt extends BaseRichBolt {
        OutputCollector _collector;
        long currentSec = -1;
        HashMap<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            long time = System.currentTimeMillis();
            long newSec = time / 1000;

            if (newSec != currentSec) {
                int wCnt = 0;
                int vCnt = 0;
                for (Map.Entry<String, Integer> entry: counts.entrySet()) {
                    wCnt ++;
                    vCnt += entry.getValue();
                    _collector.emit(tuple, new Values(entry.getKey(), entry.getValue(), currentSec * 1000 + 500));
                }
                System.out.println("words: " + wCnt + " value: " + vCnt + " Sec: " + currentSec);
                counts.clear();
                currentSec = newSec;
            }

            String[] words = tuple.getString(0).replaceAll("\n", " ").split(" ");
            if (words == null) {
                return;
            }
            for (String s: words) {
                if (!s.equals("")) {
                    int cnt = 1;
                    if (counts.containsKey(s)) {
                        cnt += counts.get(s);
                    }
                    counts.put(s, cnt);
                }
            }
            
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count", "time"));
        }
    }

    public static class GrepResultBolt extends BaseRichBolt {
        OutputCollector _collector;

        private static int WINDOW_SIZE = 30;
//        private static int ADVANCE_SIZE = 1;
        private static int WAIT_DELTA = 1;

        private static int ARRAY_MAX = WINDOW_SIZE + WAIT_DELTA;
        private long LSec = -1;
        private long RSec = -1;
        private LinkedList<Ele> WC = null;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;

            WC = new LinkedList<Ele>();
        }

        private void emitWindow(Tuple tuple) {
            Ele e = WC.getFirst();
            long leftSec = LSec;
            long rightSec = leftSec + WINDOW_SIZE;
            for (String w: e.counts.keySet()) {
                int cnt = e.counts.get(w);
                for (int k = 1; k < WC.size(); k ++) {
                    if (WC.get(k).currentSec >= rightSec) {
                        break;
                    }
                    if (WC.get(k).counts.containsKey(w)) {
                        cnt += WC.get(k).counts.get(w);
                    }
                }
                e.counts.put(w, cnt);
                _collector.emit(tuple, new Values(w, cnt, leftSec));
                System.out.println(w + " " + cnt + " " + leftSec + " - " + rightSec + " latency: " + (System.currentTimeMillis() - (WINDOW_SIZE + 1 + leftSec) * 1000));
            }
            WC.pollFirst();
            LSec = WC.get(0).currentSec;
        }

        @Override
        public void execute(Tuple tuple) {
            String w = tuple.getString(0);
            int count = tuple.getInteger(1);
            long sentTimeSec = tuple.getLong(2) / 1000;

            if (WC.size() == 0) {
                LSec = sentTimeSec - WAIT_DELTA;
                RSec = sentTimeSec;
                for (long k = LSec; k <= RSec; k ++) {
                    WC.add(new Ele(k));
                }
            }

            if (sentTimeSec < LSec) {
                System.out.println("LSec = " + LSec + " and new word timeSec = " + sentTimeSec + " . BAD &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ");
            }

            if (sentTimeSec > RSec) {
                for (long k = RSec + 1; k <= sentTimeSec; k ++) {
                    WC.add(new Ele(k));
                }
                RSec = sentTimeSec;
            }

            while (WC.size() > ARRAY_MAX) {
                emitWindow(tuple);
            }

            for (int k = 0; k < WC.size(); k ++) {
                if (WC.get(k).currentSec == sentTimeSec) {
                    WC.get(k).addWord(w, count, tuple.getLong(2));
                    break;
                }
            }
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "aggregate_count", "leftSec"));
        }
    }

    public static class TopKGrepResultBolt extends BaseRichBolt {
        OutputCollector _collector;

        private static int WINDOW_SIZE = 30;
//        private static int ADVANCE_SIZE = 1;
        private static int WAIT_DELTA = 1;
        private static int TOPK = 50;

        private static int ARRAY_MAX = WINDOW_SIZE + WAIT_DELTA;
        private long LSec = -1;
        private long RSec = -1;
        private LinkedList<Ele> WC = null;

        private boolean isFinal = false;
        public TopKGrepResultBolt(boolean tIsFinal) {
            super();
            isFinal = tIsFinal;
        }

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;

            WC = new LinkedList<Ele>();
        }

        private void emitWindow(Tuple tuple) {
            Ele e = WC.getFirst();
            long leftSec = LSec;
            long rightSec = leftSec + WINDOW_SIZE;
            TopKEle TKE = new TopKEle(LSec, TOPK);
            for (String w: e.counts.keySet()) {
                int cnt = e.counts.get(w);
                if (!isFinal) {
                    for (int k = 1; k < WC.size(); k ++) {
                        if (WC.get(k).currentSec >= rightSec) {
                            break;
                        }
                        if (WC.get(k).counts.containsKey(w)) {
                            cnt += WC.get(k).counts.get(w);
                        }
                    }
                }
                e.counts.put(w, cnt);
                TKE.addWord(w, cnt);
//                _collector.emit(new Values(w, cnt, leftSec));
//                System.out.println(w + " " + cnt + " " + leftSec + " - " + rightSec);
            }
            WC.pollFirst();
            LSec = WC.get(0).currentSec;
            for (Map.Entry<String, Integer> entry: TKE.counts.entrySet()) {
                if (!isFinal) {
                    _collector.emit(tuple, new Values(entry.getKey(), entry.getValue(), leftSec * 1000));
                } else {
                    System.out.println("TopK " + TOPK + " words: " + entry.getKey() + " " + entry.getValue() + " " + leftSec + " - " + rightSec + " latency: " + (System.currentTimeMillis() - (WINDOW_SIZE + 1 + leftSec) * 1000));
                }
            }
        }

        @Override
        public void execute(Tuple tuple) {
            String w = tuple.getString(0);
            int count = tuple.getInteger(1);
            long sentTimeSec = tuple.getLong(2) / 1000;

            if (WC.size() == 0) {
                LSec = sentTimeSec - WAIT_DELTA;
                RSec = sentTimeSec;
                for (long k = LSec; k <= RSec; k ++) {
                    WC.add(new Ele(k));
                }
            }

            if (sentTimeSec < LSec) {
                System.out.println("LSec = " + LSec + " and new word timeSec = " + sentTimeSec + " . BAD &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ");
            }

            if (sentTimeSec > RSec) {
                for (long k = RSec + 1; k <= sentTimeSec; k ++) {
                    WC.add(new Ele(k));
                }
                RSec = sentTimeSec;
            }

            if (!isFinal) {
                while (WC.size() > ARRAY_MAX) {
                    emitWindow(tuple);
                }
            } else {
                while (WC.size() > WAIT_DELTA) {
                    emitWindow(tuple);
                }
            }

            for (int k = 0; k < WC.size(); k ++) {
                if (WC.get(k).currentSec == sentTimeSec) {
                    WC.get(k).addWord(w, count, tuple.getLong(2));
                    break;
                }
            }
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "aggregate_count", "leftMs"));
        }
    }

//    public static int BOOK_PER_SEC = 100;

    private static void slidingWC(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        if(args!=null && args.length > 0) {
            int nodes = 10;
            int bookPerSec = Integer.parseInt(args[1]);
            int blocks = Integer.parseInt(args[2]);

//            builder.setSpout("SenSpout", new SentenceSource(), nodes);
            builder.setSpout("SenSpout", new BookSource(bookPerSec, blocks), nodes);
            builder.setBolt("GrepBolt", new GrepBolt(), nodes * 2)
                    .localOrShuffleGrouping("SenSpout");
            builder.setBolt("GrepResultBolt", new GrepResultBolt(), nodes)
                    .fieldsGrouping("GrepBolt", new Fields("word"));
                
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
            builder.setBolt("GrepResultBolt", new GrepResultBolt(), 1)
                    .fieldsGrouping("GrepBolt", new Fields("word"));
                
            Config conf = new Config();
            conf.setDebug(true);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("WindowTopology", conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology("WindowTopology");
            cluster.shutdown();    
        }
    }

    private static void topK(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        if(args!=null && args.length > 0) {
            int nodes = 10;
            int bookPerSec = Integer.parseInt(args[1]);
            int blocks = Integer.parseInt(args[2]);

//            builder.setSpout("SenSpout", new SentenceSource(), nodes);
            builder.setSpout("SenSpout", new BookSource(bookPerSec, blocks), nodes);
            builder.setBolt("GrepBolt", new GrepBolt(), nodes * 2)
                    .localOrShuffleGrouping("SenSpout");
            builder.setBolt("TopKGrepResultBolt", new TopKGrepResultBolt(false), nodes)
                    .fieldsGrouping("GrepBolt", new Fields("word"));
            builder.setBolt("TopKFinalResultBolt", new TopKGrepResultBolt(true), 1)
                    .localOrShuffleGrouping("TopKGrepResultBolt");

            Config conf = new Config();
//            conf.setDebug(true);

            conf.setNumWorkers(nodes * 4);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            int nodes = 2;

            builder.setSpout("SenSpout", new SentenceSource(), nodes);
//            builder.setSpout("SenSpout", new BookSource(), nodes);
            builder.setBolt("GrepBolt", new GrepBolt(), nodes)
                    .localOrShuffleGrouping("SenSpout");
            builder.setBolt("TopKGrepResultBolt", new TopKGrepResultBolt(false))
                    .fieldsGrouping("GrepBolt", new Fields("word"));
            builder.setBolt("TopKFinalResultBolt", new TopKGrepResultBolt(true), 1)
                    .localOrShuffleGrouping("TopKGrepResultBolt");

            Config conf = new Config();
//            conf.setDebug(true);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("TopKTopology", conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology("TopKTopology");
            cluster.shutdown();    
        }
    }

    public static void main(String[] args) throws Exception {
//        slidingWC(args);
        topK(args);
    }
}
