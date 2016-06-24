package com.alecspopa.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Do a sentiment analysis for each mention.
 */
public class SentimentAnalysisBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2706047697068872387L;

	private static final Logger logger = LoggerFactory.getLogger(SentimentAnalysisBolt.class);

	/** Number of seconds before the top list will be logged to stdout. */
    private final long logIntervalSec;

    /** Number of seconds before the top list will be cleared. */
    private final long clearIntervalSec;

    private Map<String, Long> sentimentScoreCounter;
    private long lastLogTime;
    private long lastClearTime;

    public SentimentAnalysisBolt(long logIntervalSec, long clearIntervalSec) {
        this.logIntervalSec = logIntervalSec;
        this.clearIntervalSec = clearIntervalSec;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    	sentimentScoreCounter = new HashMap<String, Long>();
        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple input) {
    	String mention = (String) input.getValueByField("mention");
        String word = (String) input.getValueByField("word");
        
        Long sentimentScore = sentimentScoreCounter.get(mention);
        if (sentimentScore == null) {
        	sentimentScore = 0L;
        }
        
        if (DictionaryWords.POSITIVE_WORDS.contains(word)) {
        	sentimentScore += 1;
        } else if (DictionaryWords.NEGATIVE_WORDS.contains(word)) {
        	sentimentScore -= 1;
        }
        
        sentimentScoreCounter.put(mention, sentimentScore);

        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > logIntervalSec) {
        	System.out.println("\n\n");
            publishList();
            lastLogTime = now;
        }
    }

    private void publishList() {
        // Output list:
        for (Map.Entry<String, Long> entry : sentimentScoreCounter.entrySet()) {
            logger.info(new StringBuilder("mention - ").append(entry.getKey()).append(" :: score - ").append(entry.getValue()).toString());
        }

        // Clear list
        long now = System.currentTimeMillis();
        if (now - lastClearTime > clearIntervalSec * 1000) {
        	sentimentScoreCounter.clear();
            lastClearTime = now;
        }
    }
}
