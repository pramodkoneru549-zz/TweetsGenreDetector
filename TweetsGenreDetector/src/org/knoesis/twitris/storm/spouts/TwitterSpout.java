package org.knoesis.twitris.storm.spouts;

//JDK Imports
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/*
 * This Storm Spout is responsible for distributing the "tweets" shared by the 
 * Twitter Streaming API.
 * 
 * Note: It is heavily based on Nathan Marz's TwitterSampleSpout found in the 
 * "storm-starter" project: https://github.com/nathanmarz/storm-starter 
 */
public class TwitterSpout implements IRichSpout 
{

	/**
	 * 
	 */
	private static Log log = LogFactory.getLog(TwitterSpout.class);
	private static final long serialVersionUID = 1L;
	// Declarations
	SpoutOutputCollector collector; // Used to distribute tuples (Edge)
	LinkedBlockingQueue<Status> queue = null; // Queue of Status Objects
	TwitterStream twitterStream; // Twitter Stream
	String twitterUserName; // Twitter Login User Name
	String twitterPassword; // Twitter Login Password
	String[] keywords;
	private long count = 0;

	// Constructor:
	// Sets the Authentication data to access the Twitter Streaming API.
	public TwitterSpout(String twitterUserName, String twitterPassword, ArrayList<String> keywords)
	{
		this.twitterUserName = twitterUserName;
		this.twitterPassword = twitterPassword;

		// Converting arrayList to array, since the filter method of streaming API takes
		// only array.
		this.keywords = keywords.toArray(new String[keywords.size()]);
	}

	@Override
	public void ack(Object arg0) 
	{
		// Nothing Done Here At This Point ....
	}

	@Override
	public void close() 
	{
		// Close the Twitter Streamthis.fact = fact;
		this.twitterStream.shutdown();
	}

	@Override
	public void fail(Object arg0) 
	{
		// Nothing Done Here At This Point ...
	}

	public void checkTimeConsumption(String checkPointName) {
		//		timeEnd = System.currentTimeMillis();
		//		long diff = timeEnd - timeStart;
		//		System.err.println(checkPointName + " time: " + diff);
		//		timeStart = timeEnd;
	}

	public void initTimer() {
		//		timeStart = System.currentTimeMillis();
		//		System.err.println("============INIT TIMER====================");
	}

	@Override
	public void nextTuple() 
	{
		// Grab a Status off the Queue
		Status status = queue.poll();

		// Check to make sure it's Valid (not empty?)
		if (status == null)
		{
			// If Invalid, Let's Wait for 50ms.
			Utils.sleep(50);
		}

		// If Valid, Pass the "Tweet" to the Cluster.
		else
		{
			this.collector.emit(new Values(status));
			if(++count % 1000 == 0) {
				System.out.println(new Date() + " TwitterSpout received tweets: " + count);
				System.out.println("total memory: " + Runtime.getRuntime().totalMemory()/ (1000000));

				//				checkTimeConsumption("received 20 tweets");
			}
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, 
			SpoutOutputCollector collector) 
	{
		initTimer();
		// Instantiate the Queue
		queue = new LinkedBlockingQueue<Status>(10000);



		// Specify the Output Collector
		this.collector = collector;

		// Instantiate the Twitter Status Listener and set it to add new
		// Status objects to the Queue.
		StatusListener listener = new StatusListener()
		{	
			@Override
			public void onException(Exception e) 
			{
				// Nothing done here, yet...
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice notice) 
			{
				// Nothing done here, yet...
			}

			@Override
			public void onScrubGeo(long arg0, long arg1) 
			{
				// Nothing done here, yet...
			}

			@Override
			public void onStatus(Status status) 
			{
				// Insert the Status into the Queue
				queue.offer(status);
				//System.out.println("Length of the queue" + queue.size());
			}

			@Override
			public void onTrackLimitationNotice(int notice) 
			{
				// Nothing done here, yet...
			}
		};

		// Start The Stream
		log.info("Initializing and Starting the Twitter Streaming API with username:"+this.twitterUserName);
		TwitterStreamFactory fact = new TwitterStreamFactory(
				new ConfigurationBuilder()
				.setUser(this.twitterUserName)
				.setPassword(this.twitterPassword)
				.build());
		twitterStream = fact.getInstance();
		twitterStream.addListener(listener);
		//twitterStream.sample();

		// Setup and Execute Stream Filtering
		FilterQuery filterQuery = new FilterQuery();
		filterQuery.track(keywords);

		twitterStream.filter(filterQuery);



	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) 
	{

		// Declare the Output Label (Single Tweet)
		declarer.declare(new Fields("tweet"));
	}

	public boolean isDistributed() 
	{	
		return false;
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}

