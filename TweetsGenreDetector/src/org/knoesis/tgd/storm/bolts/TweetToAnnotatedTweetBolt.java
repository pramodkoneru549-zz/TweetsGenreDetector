package org.knoesis.tgd.storm.bolts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.knoesis.tgd.database.MongoDBHandler;
import org.knoesis.tgd.extractors.LocationFetcher;
import org.knoesis.twarql.models.AnnotatedTweet;

import twitter4j.Status;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * This Bolt extracts URLs and emits a stream of triples with the tweetid.
 * Normally used when you need to parallellize the Extractors (Not in pipeline)
 * @author pavan
 *
 * TODO: Would be better if the schema for the triple are taken from a config file or an ontology/schema reader.
 */
public class TweetToAnnotatedTweetBolt implements IRichBolt{
	/**
	 * 
	 */
	private Log log = LogFactory.getLog(TweetToAnnotatedTweetBolt.class); 
	private Map<String, Set<Set<String>>> keywordsMap = new HashMap<String, Set<Set<String>>>();
	private Map<Set<String>, String> keywordEventMap = new HashMap<Set<String>, String>();
	private static final long serialVersionUID = 1L;
	private static LocationFetcher locationFetcher;
	private OutputCollector _collector;
	private MongoDBHandler dbHandler;
	@Override
	public void cleanup() {

	}

	public TweetToAnnotatedTweetBolt(Map<String, Set<Set<String>>> keywordsMap,
			Map<Set<String>, String> keywordEventMap) {
		log.info("Preparing Bolt TweetToAnnotatedTweetBolt");
		
		locationFetcher = new LocationFetcher();
		this.keywordEventMap= keywordEventMap;
		this.keywordsMap = keywordsMap;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		dbHandler = new MongoDBHandler();
		
	}

	@Override
	public void execute(Tuple input) {
		Status status = (Status)input.getValue(0);
		AnnotatedTweet aTweet = new AnnotatedTweet(status);
		
		//Check Event here and add the event to the annotated tweet
		aTweet.setEvent(checkEvent(aTweet.getStatusTweet().getText()));
		
		//Fetching the location for the tweet
		locationFetcher.process(aTweet);
		log.debug("Emmitting the tweet after transformation to AnnotatedTweet:" +status.getId());
		_collector.emit(input, new Values(status.getId(), aTweet));
		_collector.ack(input);
	}

	/**
	 * Returns the Event for the tweet. 
	 * 
	 * TODO: This has has to be transformed to Set<Events> 
	 * TODO: You can use TRIE Spotter here to Check for substring match. An optimized daatastructure. 
	 * @param tweetText
	 * @return
	 */
	private String checkEvent(String tweetText){
		log.debug("Check Event of the Tweet ");
		String tweetLowerCase = tweetText.toLowerCase();
		String[] tweetContent = tweetLowerCase.split("\\W+");

		// Storing tweet Content into a set.
		Set<String> tweetContentSet = new HashSet<String>(Arrays.asList(tweetContent));
		String eventID = null;
		for (String word : tweetContentSet) {
			if (keywordsMap.containsKey(word)) {
				for (Set<String> keywordset : keywordsMap.get(word)) {

					// Checking if the keyword set is in the tweet set.
					if (tweetContentSet.containsAll(keywordset)) {
						eventID = keywordEventMap.get(keywordset);
					}
				}
			}
		}
		return eventID;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "annotated tweet"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}