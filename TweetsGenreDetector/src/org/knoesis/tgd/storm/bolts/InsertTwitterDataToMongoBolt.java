package org.knoesis.tgd.storm.bolts;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.knoesis.tgd.database.MongoDBHandler;
import org.knoesis.twarql.models.AnnotatedTweet;

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
 * 
 * @author pavan
 * 
 *         TODO: Would be better if the schema for the triple are taken from a
 *         config file or an ontology/schema reader.
 */
public class InsertTwitterDataToMongoBolt implements IRichBolt {
	/**
	 * 
	 */
	private static Log log = LogFactory.getLog(InsertTwitterDataToMongoBolt.class);
	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;
	private long tweetCount = 0l;
	private static int INSERT_COUNT = 20;
	private MongoDBHandler dbHandler = null;
	private List<AnnotatedTweet> aTweets = null;

	@Override
	public void cleanup() {
		dbHandler.closeConnection();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		log.info("Preparing Bolt InsertTwitterDataBolt");
		aTweets = new ArrayList<AnnotatedTweet>();
		dbHandler = new MongoDBHandler();
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		AnnotatedTweet aTweet = (AnnotatedTweet) input.getValueByField("annotated tweet");
		aTweets.add(aTweet);
		tweetCount++;
		if (tweetCount % INSERT_COUNT == 0) {
			log.debug("Inserting twitter data, author location, location data for " + tweetCount
					+ " tweets");
			dbHandler.batchInsertTwitterData(aTweets);
			// checkTimeConsumption("insertTwitterData");
//			dbHandler.batchInsertAuthorLocationData(aTweets);
			// checkTimeConsumption("insertAuthorLocationData");
//			dbHandler.batchInsertLocationLatLongData(aTweets);
			// checkTimeConsumption("insertLocationLatLongData");

			// dbHandler.closeConnection();
			aTweets.clear();
		}
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("annotated tweet"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}