package lex.microsoft.com;

import java.util.Map;
import java.util.Date;
import java.util.ArrayList;
import java.util.Iterator;
import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class SlidingBolt extends BaseBasicBolt {
  //For holding sensor entry counts
  ArrayList entries = new ArrayList();

  @Override
  public Map<String, Object> getComponentConfiguration() {
      Config conf = new Config();
      int tickFrequencyInSeconds = 1;
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
      return conf;
  }    

//execute is called to process tuples
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

      if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
         && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)){
         System.out.println("********TICK_TUPLE RECEIVED*******");
         Date d = new Date();
         Long time = d.getTime();
         Integer before = entries.size();
         Iterator i = entries.iterator();
         while(i.hasNext()){
           Long t = (Long) i.next();
           if(t < (time - 10000)){
             i.remove();
           }
         }
         Integer after= entries.size();
         Integer deleted = before - after;
         System.out.println(Integer.toString(deleted) + " deleted.");
          
        } else {
          Date d = new Date();
          entries.add(d.getTime());
      }
          Integer count = entries.size();
          System.out.println(Integer.toString(count) + " entries in last 10 seconds");

         //Emit the sensor and the count of entries in the last 10 seconds
          collector.emit(new Values(count));
    }

    //Declare that we will emit a tuple containing a single field
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("entries"));
    }
}