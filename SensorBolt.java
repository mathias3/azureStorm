package lex.microsoft.com;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class SensorBolt extends BaseBasicBolt {
  //For holding sensor entry counts
    Map<String, Integer> entries = new HashMap<String, Integer>();

    //execute is called to process tuples
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      //Get the sensor name from the tuple
      String sensor = tuple.getString(0);
      Integer count = entries.get(sensor);
      if (count == null)
        count = 0;
      //Increment the count and store it
      count++;
      entries.put(sensor, count);
      //Emit the sensor and the current count of entries
      collector.emit(new Values(sensor, count));
    }

    //Declare that we will emit a tuple containing two fields; sensor and entries
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("sensor", "entries"));
    }
}