package com.h3c.storm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.hbase.bolt.mapper.HBaseMapper;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class PersistTopology
{

	private static final String	KAFKA_SPOUT	= "KAFKA_SPOUT";
	private static final String	HBASE_BOLT	= "HBASE_BOLT";

	public static void main(String[] args) throws Exception
	{

		/* define spout */
		KafkaSpout kafkaSpout = new KafkaSpout();

		System.setProperty("hadoop.home.dir", "E:\\eclipse\\");

		/* define HBASE Bolt */
		HBaseMapper mapper = new MyHBaseMapper();
		PrivateHBaseBolt hbaseBolt = new PrivateHBaseBolt("testhbasebolt", mapper).withConfigKey("hbase.conf");

		/* define topology */
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(KAFKA_SPOUT, kafkaSpout);
		builder.setBolt(HBASE_BOLT, hbaseBolt, 2).shuffleGrouping(KAFKA_SPOUT);

		Config conf = new Config();
		conf.setDebug(true);

		Map<String, Object> hbConf = new HashMap<String, Object>();
		// if(args.length > 0){
		// //hbConf.put("hbase.rootdir", args[0]);
		// hbConf.put("hbase.rootdir",
		// "hdfs://node1.hde.h3c.com:8020/apps/hbase/data");
		// }

		conf.put("hbase.conf", hbConf);

		// conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		// conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		// conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		// conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);

		if (args != null && args.length > 0)
		{
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else
		{

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(60000000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}