package org.knoesis.tgd.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.knoesis.twarql.models.ATweetGeoLocation;
import org.knoesis.twarql.models.AnnotatedTweet;
import org.knoesis.tgd.config.ConfigManager;

import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.User;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;


/**
 * This class contains all the methods related to inserting/deletion/updating
 * data into mongoDB
 * @author pramodkoneru549
 *
 */
public class MongoDBHandler implements DBHandler {


	private Mongo mongo= null;
	private DB mongoDatabase = null;
	private static Log log = LogFactory.getLog(MongoDBHandler.class);
	
	/**
	 * Initiate the connection
	 */
	public MongoDBHandler(){
		Properties p = ConfigManager.getInstance().getProperties();
		String host = p.getProperty("mongodb.hostname");
		String dbname = p.getProperty("mongodb.database");

		try {
			mongo = new Mongo(host);
			mongoDatabase = mongo.getDB(dbname); //creates if it does not exist
		} catch (Exception e) {
			//log it and fire a run time exception
			log.error("Mongo creation failed", e);
			throw new RuntimeException(e);
		}

	}

	/**
	 * This method given it the list of annotated tweets enters that data into Mongo DB
	 */
	public void batchInsertTwitterData(List<AnnotatedTweet> aTweets) {

		DBCollection collection = mongoDatabase.getCollection("tweets");
		List<DBObject> objectList = new ArrayList<DBObject>();
		for (AnnotatedTweet tweet : aTweets) {
			DBObject dbObject = new BasicDBObject();
			Status tweetStatus = tweet.getStatusTweet();
			//`twitter_ID`, `tweet_text`, `eventID`, `published_date`,`twitter_author`, `latitude`, `longitude`
			dbObject.put("twitter_id" , tweetStatus.getId());
			dbObject.put("twitter_text" , tweetStatus.getText());
			dbObject.put("eventID" , tweet.getEvent());
			dbObject.put("published_date",tweetStatus.getCreatedAt());
			dbObject.put("twitter_author",getAuthorObject(tweetStatus.getUser())); //nested
			dbObject.put("location",getLocationObject(tweet.getGeoLocation())); //nested field

			if (tweetStatus.getInReplyToStatusId() > 0){
				dbObject.put("reply_to_twitter_ID" , tweetStatus.getInReplyToStatusId());
				dbObject.put("reply_to_author_ID",tweetStatus.getInReplyToUserId());
			}

			if (tweetStatus.isRetweet()){
				//create a nested object for retweet
				dbObject.put("retweet", getRetweetObject(tweetStatus.getRetweetedStatus()));
			}
			objectList.add(dbObject);
		}

		collection.insert(objectList);

	}

	private DBObject getRetweetObject(Status retweetStatus){
		DBObject dbObject = new BasicDBObject();
		dbObject.put("twitter_id" , retweetStatus.getId());
		dbObject.put("twitter_text" , retweetStatus.getText());
		dbObject.put("published_date",retweetStatus.getCreatedAt());
		dbObject.put("twitter_author",getAuthorObject(retweetStatus.getUser())); //nested
		dbObject.put("location",getLocationObject(retweetStatus.getGeoLocation())); //nested field
		return dbObject;
	}

	/**
	 * Get a mongdb object for twitter4j location
	 * @return
	 */
	private DBObject getLocationObject(GeoLocation locationObj){
		DBObject dbObject = new BasicDBObject();
		dbObject.put("latitude", locationObj.getLatitude());
		dbObject.put("longitude", locationObj.getLongitude());
		return dbObject;
	}
	/**
	 * Get a mongdb object for location
	 * @return
	 */
	private DBObject getLocationObject(ATweetGeoLocation locationObj){
		DBObject dbObject = new BasicDBObject();
		dbObject.put("latitude", locationObj.getLatitude());
		dbObject.put("longitude", locationObj.getLongitude());
		return dbObject;
	}

	/**
	 * Get a mongo object for author given a twitter4j user object
	 * @param user
	 * @return
	 */
	private DBObject getAuthorObject(User user){
		DBObject dbObject = new BasicDBObject();
		dbObject.put("screen_name", user.getScreenName());

		return dbObject;
	}
	@Override
	public void batchInsertTwitterDataForAllMetadata(
			List<AnnotatedTweet> aTweets) {
		//just call the other method
		batchInsertTwitterData(aTweets);

	}

	@Override
	public void batchInsertAuthorLocationData(List<AnnotatedTweet> aTweets) {
		// TODO Auto-generated method stub

	}

	@Override
	public void batchInsertLocationLatLongData(List<AnnotatedTweet> aTweets) {
		// TODO Auto-generated method stub

	}

	@Override
	public void batchInsertHashTags(List<AnnotatedTweet> aTweets) {
		// TODO Auto-generated method stub

	}

	@Override
	public void batchInsertUrls(List<AnnotatedTweet> aTweets) {
		// TODO Auto-generated method stub

	}

	@Override
	public void batchInsertRelatedTweets(List<AnnotatedTweet> aTweets) {
		// TODO Auto-generated method stub

	}

	@Override
	public ATweetGeoLocation getGeoLocationData(AnnotatedTweet aTweet) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isSuccessQueryExecute(int[] returnCode) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}