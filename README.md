Apache Storm Twitter Naive Sentiment Analysis
====

Sample project based on https://github.com/davidkiss/storm-twitter-word-count demonstrating real-time computation of Apache Storm framework.

The code subscribes to Twitter's Sample feed, and does some simple sentiment analysis of the feed for different #hastags. 
It keeps stats of hashtags occuring in tweets and logs the list every 10 seconds.

=== To get started:
* Clone this repo
* Import as existing Maven project in Eclipse
* Run Topology.java with your twitter credentials as VM args (see http://twitter4j.org/en/configuration.html#systempropertyconfiguration)

You'll need to have valid Twitter OAuth credentials to get the sample working.
For the exact steps on how to do that, visit https://dev.twitter.com/discussions/631.

=== Sample run command:

	mvn exec:java -Dexec.mainClass="com.alecspopa.storm.Topology" \
		-Dtwitter4j.oauth.consumerKey=****** \
		-Dtwitter4j.oauth.consumerSecret=****** \
		-Dtwitter4j.oauth.accessToken=****** \
		-Dtwitter4j.oauth.accessTokenSecret=******
	