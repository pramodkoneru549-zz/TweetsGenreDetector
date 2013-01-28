package org.knoesis.tgd.storm.topologies;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.knoesis.tgd.config.ConfigManager;
import org.knoesis.tgd.storm.bolts.InsertTwitterDataToMongoBolt;
import org.knoesis.tgd.storm.bolts.TweetToAnnotatedTweetBolt;
import org.knoesis.tgd.utils.EventIdKeywordsGenerator;
import org.knoesis.twitris.storm.spouts.TwitterSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class TwitrisTopology {
	static Log log = LogFactory.getLog(TwitrisTopology.class);
	static Map<String, String[]> eventsKeywordshashmap;

	static Map<String, Set<Set<String>>> keywordsMap = new HashMap<String, Set<Set<String>>>();
	static Map<Set<String>, String> keywordEventMap = new HashMap<Set<String>, String>();

	// List to store all the keywords for the file, for the streaming API to
	// crawl for the tweets.
	static ArrayList<String> totalkeywords = new ArrayList<String>();

	public static void main(String[] args) {
		// for config parameters taken from input properties file, set to
		// default folder data/event_keywords.properties
		ConfigManager configParams = ConfigManager.getInstance();

		EventIdKeywordsGenerator eventKeywordsDataGenerator = new EventIdKeywordsGenerator(
				configParams);
		keywordsMap = eventKeywordsDataGenerator.getKeywordsMap();
		keywordEventMap = eventKeywordsDataGenerator.getkeywordEventMap();
		totalkeywords = eventKeywordsDataGenerator.getTotalKeywords();

		// Twitter Authentication UserName and Password.
		String userName = "";
		String password = "";
		if (!configParams.isClusterModeLocal()) {
			userName = configParams.getTwitterUsername();// "knoesisstreaming";
			password = configParams.getTwitterPassword(); // "knoesis12345";
		} else {
			// FIXME for now using same for both of them
			// Modify this later
			userName = configParams.getTwitterUsername(); // "streamingpramod";
			password = configParams.getTwitterPassword(); // "kon007";
		}

		// Construct the Topology
		log.info("Bulding the Topology for Twitris Crawler");
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("1", new TwitterSpout(userName, password, totalkeywords));
		TweetToAnnotatedTweetBolt tweetToAnnotatedTweet = new TweetToAnnotatedTweetBolt(
				keywordsMap, keywordEventMap);
		builder.setBolt("2", tweetToAnnotatedTweet, 1).shuffleGrouping("1");
		builder.setBolt("3", new InsertTwitterDataToMongoBolt(), 1).shuffleGrouping("2");
		log.debug("Done -- Bulding the Topology for Twitris Crawler");

		// Choose Local or Remote Deployment
		boolean localMode = configParams.isClusterModeLocal();// true;
		// LocalMode Deployment
		if (localMode) {
			log.info("Running the crawler in local mode");
			// Setup the Configuration
			Config conf = new Config();
			conf.setNumWorkers(1);

			// Setup the Local Cluster for Testing Purposes
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("TwitrisTopology_new", conf, builder.createTopology());
		}

		// Remote Deployment
		else {
			log.info("Running the crawler in remote mode");
			Config conf = new Config();
			conf.setNumWorkers(1);

			conf.setMaxSpoutPending(5000);
			conf.setDebug(true);
			try {
				StormSubmitter
						.submitTopology("TwitrisTopology_new", conf, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
