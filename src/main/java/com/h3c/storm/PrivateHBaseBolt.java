package com.h3c.storm;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Constants;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PrivateHBaseBolt extends AbstractPrivateHBaseBolt
{
	private static final Logger	LOG			= LoggerFactory.getLogger(PrivateHBaseBolt.class);
	boolean						writeToWAL	= true;
	List<Mutation>				batchMutations;
	List<Tuple>					tupleBatch;

	/* @new add */
	private static final int	BATCHSIZE	= 10000;

	public PrivateHBaseBolt(String tableName, HBaseMapper mapper)
	{
		super(tableName, mapper);
		this.batchMutations = new LinkedList<>();
		this.tupleBatch = new LinkedList<>();
	}

	public PrivateHBaseBolt writeToWAL(boolean writeToWAL)
	{
		this.writeToWAL = writeToWAL;
		return this;
	}

	public PrivateHBaseBolt withConfigKey(String configKey)
	{
		this.configKey = configKey;
		return this;
	}

	public static boolean isTick(Tuple tuple)
	{
		return tuple != null && Constants.SYSTEM_COMPONENT_ID.equals(tuple.getSourceComponent())
				&& Constants.SYSTEM_TICK_STREAM_ID.equals(tuple.getSourceStreamId());
	}

	@Override
	public void execute(Tuple tuple)
	{
		boolean flush = false;

		try
		{
			if (PrivateHBaseBolt.isTick(tuple))
			{
				System.err.println("TICK received!");
				collector.ack(tuple);
				flush = true;
			} else
			{
				byte[] rowKey = this.mapper.rowKey(tuple);
				ColumnList cols = this.mapper.columns(tuple);
				List<Mutation> mutations = hBaseClient.constructMutationReq(rowKey, cols,
						writeToWAL ? Durability.SYNC_WAL : Durability.SKIP_WAL);
				batchMutations.addAll(mutations);
				tupleBatch.add(tuple);
				if (tupleBatch.size() >= BATCHSIZE)
				{
					flush = true;
				}
			}

			if (flush && !tupleBatch.isEmpty())
			{
				this.hBaseClient.batchMutate(batchMutations);
				for (Tuple t : tupleBatch)
				{
					collector.ack(t);
				}
				tupleBatch.clear();
				batchMutations.clear();
			}
		} catch (Exception e)
		{
			System.err.println("err happen!");
			this.collector.reportError(e);
			for (Tuple t : tupleBatch)
			{
				this.collector.fail(t);
			}
			tupleBatch.clear();
			batchMutations.clear();
		}

		// this.collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
	{

	}
}
