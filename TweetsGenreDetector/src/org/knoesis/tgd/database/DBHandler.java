package org.knoesis.tgd.database;

import java.util.List;

import org.knoesis.twarql.models.ATweetGeoLocation;
import org.knoesis.twarql.models.AnnotatedTweet;



/**
 * Acts as the interface for the DB Handling tasks
 * @author ajith
 *
 */
public interface DBHandler {

	/**
	 * This function batch inserts the twitter data 
	 * also includes the latitude and longitude 
	 * @param aTweets
	 */
	void batchInsertTwitterData(List<AnnotatedTweet> aTweets);



	/**
	 * This function batch inserts the twitter data for all the available metadata
	 * includes: `twitter_ID` ,`tweet` ,`eventID` ,`published_date` ,`twitter_author` ,`latitude` ,`longitude` ,
	 *           `reply_to_twitter_ID` ,`rt_twitter_ID` ,`author_ID` ,`resolved_urls` ,`retweet_count` ,`entity_mentions` ,`hashtags`)
	 * @param aTweets
	 * Contact @author Hemant
	 */
	void batchInsertTwitterDataForAllMetadata(List<AnnotatedTweet> aTweets);	
	/**
	 * This function inserts the author locations if it is 
	 * not already present in the DB
	 * @param aTweets
	 */
	public void batchInsertAuthorLocationData(List<AnnotatedTweet> aTweets) ;	

	/**
	 * Inserts the location data
	 * @param aTweets
	 */
	public void batchInsertLocationLatLongData(List<AnnotatedTweet> aTweets);


	/**
	 * This function insets the list of tags with tweetIds
	 * @author pavan
	 * @param tweetIdTags
	 */
	public void batchInsertHashTags(List<AnnotatedTweet> aTweets);

	/**
	 * Inserting the url in the database very similar to the previous function of inserting 
	 * tags. Both of them can be done as a single function with different parameters.
	 * 
	 * TODO: For now tags and urls insertion have duplicated code. Need to find reusable code and 
	 * 		 get things working better. 
	 * @param url
	 * @return
	 */
	public void batchInsertUrls(List<AnnotatedTweet> aTweets);

	/**
	 * This method inserts the tweet into the related tweets table
	 * 
	 * The table is used for faster access for the front end.
	 * @param aTweets
	 */
	public void batchInsertRelatedTweets(List<AnnotatedTweet> aTweets);


	public ATweetGeoLocation getGeoLocationData(AnnotatedTweet aTweet);

	/**
	 * Checks whether the statement has been 
	 * executed successfully or not
	 * @param returnCode
	 * @return
	 */
	public boolean isSuccessQueryExecute(int[] returnCode);


	/**
	 * Clean up stuff. Like clearing connections etc
	 */
	public void cleanup();

}
