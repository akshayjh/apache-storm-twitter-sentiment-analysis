package com.alecspopa.storm;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Topology class that sets up the Storm topology for this sample.
 * Please note that Twitter credentials have to be provided as VM args, otherwise you'll get an Unauthorized error.
 * @link http://twitter4j.org/en/configuration.html#systempropertyconfiguration
 */
public class Topology {

	static final String TOPOLOGY_NAME = "apache-storm-twitter-sentiment-analysis";

	public static void main(String[] args) {
		Set<String> languages = new HashSet<String>(Arrays.asList(new String[] {"en"}));
		Set<String> hashtags = new HashSet<String>(Arrays.asList(new String[] {
				"brexit", "barackobama", "hillaryclinton", "donaldtrump"
		}));
		Set<String> mentions = new HashSet<String>(Arrays.asList(new String[] {
				"barackobama", "hillaryclinton", "realdonaldtrump"
		}));
		
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
        b.setBolt("MentionBolt", new MentionBolt(languages, hashtags, mentions)).shuffleGrouping("TwitterSampleSpout");
        b.setBolt("TweetWordSplitterBolt", new TweetWordSplitterBolt(3)).shuffleGrouping("MentionBolt");
        b.setBolt("SentimentAnalysisBolt", new SentimentAnalysisBolt(10, 10 * 60)).shuffleGrouping("TweetWordSplitterBolt");

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});

	}

}
