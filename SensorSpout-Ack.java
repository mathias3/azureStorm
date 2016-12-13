package lex.microsoft.com;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

import java.util.HashMap;


//This spout randomly emits sensor readings
public class SensorSpout extends BaseRichSpout {
  //Collector used to emit output
  SpoutOutputCollector _collector;
  //Used to generate a random number
  Random _rand;

  // Cache for emitted tuples
  Map<String, Values> cache = new HashMap<String, Values>();


  //Open is called when an instance of the class is created
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    //Set the instance collector to the one passed in
    _collector = collector;
    //For randomness
    _rand = new Random();

  }

  //Emit data to the stream
  @Override
  public void nextTuple() {
    //Sleep for a bit
    Utils.sleep(100);
    // Generate a random sensor (0 to 5) to represent a turnstile
    Integer sensorNumber = _rand.nextInt(4) + 1;
    String sensorName = "Turnstile " + String.valueOf(sensorNumber);

    // Create a values object
    Values values = new Values(sensorName);

    // generate an ID
    String id = values.toString().hashCode()+"_"+System.currentTimeMillis();

    // cache the tuple and its id 
    cache.put(id, values);

    //Emit the values
    _collector.emit(values, id);

  }

  
  @Override
  public void ack(Object id) {
    // remove item from cache
    System.out.println(id +" acked, so remove it...");
    cache.remove(id);
  }

  
  @Override
  public void fail(Object id) {
    Values values = cache.get(id);
    if(values != null){
      _collector.emit(values, id);
      System.out.println("\tReplay: " + values);
    }
  }

  //Declare the output fields.
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sensor"));
  }
}