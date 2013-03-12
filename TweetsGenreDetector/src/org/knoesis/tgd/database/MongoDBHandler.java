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
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;

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
	 * FIXME Have to still modularize move the object creation to another method
	 */
	public void batchInsertTwitterData(List<AnnotatedTweet> aTweets) {

		DBCollection collection = mongoDatabase.getCollection("tweets");
		List<DBObject> objectList = new ArrayList<DBObject>();
		
		for (AnnotatedTweet tweet : aTweets) {
			DBObject dbObject = new BasicDBObject();
			Status tweetStatus = tweet.getStatusTweet();
			
			// Getting hashtags
			HashtagEntity[] hashtagEntities = tweetStatus.getHashtagEntities();
			ArrayList<String> hashtags = getHashtagEntities(hashtagEntities);
			if(hashtags != null){
				dbObject.put("hashtags", hashtags);
			}
			
			// Getting URL Objects
			ArrayList<DBObject> urlEntityObjects = getURLEntitiesObjects(tweetStatus.getURLEntities());
			if(urlEntityObjects != null){
				dbObject.put("urls", urlEntityObjects);
			}
			
			// Getting userMentions
			ArrayList<DBObject> userMentionEntityObjects = getUserMentionEntitiesObjects(tweetStatus.getUserMentionEntities());
			if(userMentionEntityObjects != null){
				dbObject.put("user_mentions", userMentionEntityObjects);
			}
			//`twitter_ID`, `tweet_text`, `eventID`, `published_date`,`twitter_author`, `latitude`, `longitude`
			dbObject.put("twitter_id" , tweetStatus.getId());
			dbObject.put("twitter_text" , tweetStatus.getText());
			dbObject.put("eventID" , tweet.getEvent());
			dbObject.put("published_date",tweetStatus.getCreatedAt());
			dbObject.put("twitter_author",getAuthorObject(tweetStatus.getUser())); //nested
			
			BasicDBObject locationObject = (BasicDBObject) getLocationObject(tweet.getGeoLocation());
			if(locationObject != null)
				dbObject.put("location",locationObject); //nested field

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

	/**
	 * This method take in twitter4j HashtagEntities, gets just the hashtags from them
	 * and returns an arrayList of hashtags. Returns null if there are no hashtagentities
	 * @param hashTagEntites
	 * @return
	 */
	private ArrayList<String> getHashtagEntities(HashtagEntity[] hashTagEntites){
		ArrayList<String> hashtags = null;
		int noOfHashtagEntities = hashTagEntites.length;
		if(noOfHashtagEntities > 0){
			hashtags = new ArrayList<String>();
			for (int i = 0; i < noOfHashtagEntities; i++) {
				HashtagEntity hashtagEntity = hashTagEntites[i];
				hashtags.add(hashtagEntity.getText());
			}
		}
		return hashtags;
	}

	/**
	 * This method takes in the twitter4j URLENtities and returns list of 
	 * urlDBObjects for insertion into MongoDb
	 * NOTE: Here only required fields are taken. Refer twiiter4j to know 
	 *       about all other fields available.
	 * @param urlEntities
	 * @return
	 */
	private ArrayList<DBObject> getURLEntitiesObjects(URLEntity[] urlEntities){
		ArrayList<DBObject> urlEntitiesObjects = null;
		int noOfUrlEntities = urlEntities.length;
		if(noOfUrlEntities > 0){
			urlEntitiesObjects = new ArrayList<DBObject>();
			for (int i = 0; i < urlEntities.length; i++) {
				URLEntity urlEntity = urlEntities[i];
				DBObject urlObject = new BasicDBObject();
				urlObject.put("url", urlEntity.getURL().toString());
				urlObject.put("display_url", urlEntity.getDisplayURL().toString());
				urlObject.put("expanded_url", urlEntity.getExpandedURL().toString());
				urlEntitiesObjects.add(urlObject);
			}
		}
		return urlEntitiesObjects;
	}

	/**
	 * This method takes in the twitter4j UserMentionEtities and returns list of 
	 * userMentionDBObjects for insertion into MongoDb
	 * NOTE: Here only required fields are taken. Refer twiiter4j to know 
	 *       about all other fields available.
	 * @param userMentionEntities
	 * @return
	 */
	private ArrayList<DBObject> getUserMentionEntitiesObjects(UserMentionEntity[] userMentionEntities){
		ArrayList<DBObject> userMentionObjects = null;
		int noOfUserMentionEntities = userMentionEntities.length;
		if(noOfUserMentionEntities > 0){
			userMentionObjects = new ArrayList<DBObject>();
			for (int i = 0; i < userMentionEntities.length; i++) {
				UserMentionEntity userMentionEntity = userMentionEntities[i];
				DBObject userMentionObject = new BasicDBObject();
				userMentionObject.put("id", userMentionEntity.getId());
				userMentionObject.put("screen_name", userMentionEntity.getScreenName());
				userMentionObject.put("name", userMentionEntity.getName());
				userMentionObjects.add(userMentionObject);
			}
		}
		return userMentionObjects;
	}

	private DBObject getRetweetObject(Status retweetStatus){
		DBObject dbObject = new BasicDBObject();
		dbObject.put("twitter_id" , retweetStatus.getId());
		dbObject.put("twitter_text" , retweetStatus.getText());
		dbObject.put("published_date",retweetStatus.getCreatedAt());
		dbObject.put("twitter_author",getAuthorObject(retweetStatus.getUser())); //nested
		//		dbObject.put("location",getLocationObject(retweetStatus.getGeoLocation())); //nested field
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
		DBObject dbObject = null;
		//		new BasicDBObject();
		if(locationObj.getLatitude() != 10000 || locationObj.getLongitude() != 10000){
			dbObject = new BasicDBObject();
			dbObject.put("latitude", locationObj.getLatitude());
			dbObject.put("longitude", locationObj.getLongitude());
		}
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
		dbObject.put("name", user.getName());
//		String url = user.getURL().toString();
//		if(url != null)
//			dbObject.put("url", url);
		dbObject.put("created_at", user.getCreatedAt());
		dbObject.put("favourites_count", user.getFavouritesCount());
		dbObject.put("followers_count", user.getFollowersCount());
		dbObject.put("friends_count", user.getFriendsCount());
		dbObject.put("id", user.getId());
		dbObject.put("location", user.getLocation());
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

	/**
	 * Method to close mongo connection
	 */
	public void closeConnection(){
		mongo.close();
	}

}