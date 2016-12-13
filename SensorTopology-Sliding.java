package lex.microsoft.com;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import lex.microsoft.com.SensorSpout;

public class SensorTopology {

  //Entry point for the topology
  public static void main(String[] args) throws Exception {
  //Used to build the topology
    TopologyBuilder builder = new TopologyBuilder();
    //Add the spout, with 2 executors
    builder.setSpout("sensorspout", new SensorSpout(), 2);
    //Add the SensorBolt bolt, with 5 executors
    //fieldsgrouping ensures that the same sensor is sent to the same bolt instance
    builder.setBolt("sensorbolt", new SensorBolt(), 5).fieldsGrouping("sensorspout", new Fields("sensor"));
    //Add the Sliding bolt, with 1 executors
    builder.setBolt("slidingbolt", new SlidingBolt(), 1).shuffleGrouping("sensorspout");

    //new configuration
    Config conf = new Config();
    conf.setDebug(true);

    //If there are arguments, we are running on a cluster
    if (args != null && args.length > 0) {
      //parallelism hint to set the number of workers
      conf.setNumWorkers(3);
      //submit the topology
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    //Otherwise, we are running locally
    else {
      //Cap the maximum number of executors that can be spawned
      //for a component to 3
      conf.setMaxTaskParallelism(3);
      //LocalCluster is used to run locally
      LocalCluster cluster = new LocalCluster();
      //submit the topology
      cluster.submitTopology("SensorTopology", conf, builder.createTopology());
      //sleep
      Thread.sleep(60000);
      //shut down the cluster
      cluster.shutdown();
    }
  }
}