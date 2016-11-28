package com.h3c.storm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaSpout extends BaseRichSpout
{
	private SpoutOutputCollector					collector;
	private ConsumerConnector						consumer;
	private String									topic;
	Map<String, List<KafkaStream<byte[], byte[]>>>	consumerMap;

	static int										spoutCount	= 0;

	private static ConsumerConfig createConsumerConfig()
	{
		Properties props = new Properties();
		props.put("zookeeper.connect", "172.27.8.111:2181,172.27.8.112:2181,172.27.8.119:2181");
		props.put("group.id", "group1");
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
	{
		System.err.println("open!!!!!!!!!!!!!!!");
		this.collector = collector;

		/* create consumer */
		this.topic = "historyclients";
		this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());

		/* topic HashMap,which means the map can include multiple topics */
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		this.consumerMap = consumer.createMessageStreams(topicCountMap);
	}

	@Override
	public void nextTuple()
	{

		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		String toSay = "";
		while (it.hasNext())
		{
			toSay = new String(it.next().message());
			System.err.println("receive��" + toSay);
			this.collector.emit(new Values(toSay));
			spoutCount++;
			System.err.println("spout emit��" + spoutCount);
			if (0 == spoutCount % 10000)
			{
				Utils.sleep(1000);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("clientInfo"));
	}
}
