package com.alecspopa.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.UserMentionEntity;

import java.util.Map;
import java.util.Set;

/**
 * Receives tweets and emits its words over a certain length.
 */
public class MentionBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5151173513759399636L;
	
    private OutputCollector collector;

    private Set<String> languages;
    private Set<String> hashtags;
    private Set<String> mentions;
    
    public MentionBolt(Set<String> languages, Set<String> hashtags, Set<String> mentions) {
        this.languages = languages;
        this.hashtags = hashtags;
        this.mentions = hashtags;
    }
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet");
        String lang = tweet.getUser().getLang();
        String text = tweet.getText().replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
        
        if (!this.languages.contains(lang)) {
            return;
        }
        
        UserMentionEntity mentions[] = tweet.getUserMentionEntities();
        for (UserMentionEntity mention : mentions) {
        	String mentionUserText = mention.getName().toLowerCase();
        	
        	if (!this.mentions.contains(mentionUserText)) {
        		continue;
        	}
        	
            collector.emit(new Values(mentionUserText, text));
        }
        
        
        HashtagEntity hashtags[] = tweet.getHashtagEntities();
        for (HashtagEntity hashtag : hashtags) {
        	String hashtagText = hashtag.getText().toLowerCase();
        	
        	if (!this.hashtags.contains(hashtagText)) {
        		continue;
        	}
        	
            collector.emit(new Values(hashtagText, text));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("mention", "text"));
    }
}
