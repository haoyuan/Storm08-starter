package storm.starter;

import backtype.storm.Config;
import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Random;
import java.util.*;
import java.lang.*;
import java.net.*;
import org.apache.log4j.Logger;


public class SentenceSource extends BaseRichSpout {
    public static Logger LOG = Logger.getLogger(SentenceSource.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    private long startSec = System.currentTimeMillis() / 1000;
    private long sent = 0;
    String computername = null;

    public SentenceSource() {
        this(true);

        try{
            computername = InetAddress.getLocalHost().getHostAddress() + ":" + System.currentTimeMillis();
        } catch (Exception e) {
            System.out.println("Exception caught = " + e.getMessage());
        }
    }

    public SentenceSource(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
        
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }
    
    public void close() {
    }
        
    public void nextTuple() {
        long currentSec = System.currentTimeMillis() / 1000;
        while (sent < SEN_PER_SEC * (currentSec - startSec)) {
//            _collector.emit(new Values(SEN, computername, System.currentTimeMillis()));
            _collector.emit(new Values(SEN));
            sent ++;
            currentSec = System.currentTimeMillis() / 1000;
            if (SEN_PER_SEC * (currentSec - startSec) - sent > SEN_PER_SEC * 15) {
                throw new IllegalArgumentException("The sending speed can not catch the predefined value. @ " + (currentSec - startSec) + " sec.");
            }
        }
        long currentMs = System.currentTimeMillis() / 1000;
        Utils.sleep((currentMs * 1000 + 1000) - System.currentTimeMillis());
    }
    
    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sen"));
//        declarer.declare(new Fields("sen", "ip", "time"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }

    private static int SEN_PER_SEC = 2;
    private static String SEN = "The medium researcher counts around the pinched troop The empire breaks Matei Matei announces HY with a theorem";
}
