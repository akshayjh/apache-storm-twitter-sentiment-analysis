package com.alecspopa.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Receives tweets and emits its words over a certain length.
 */
public class TweetWordSplitterBolt extends BaseRichBolt {
	private static final long serialVersionUID = 4059669784620129503L;

	private final int minWordLength;

    private OutputCollector collector;

    public TweetWordSplitterBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
    	String mention = (String) input.getValueByField("mention");
        String text = (String) input.getValueByField("text");
        
        String[] words = text.split(" ");
        for (String word : words) {
            if (word.length() >= minWordLength) {
                collector.emit(new Values(mention, word));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("mention", "word"));
    }
}