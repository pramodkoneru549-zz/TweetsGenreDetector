package org.knoesis.tgd.extractors;


import java.util.Set;

import org.knoesis.twarql.models.ATweetGeoLocation;
import org.knoesis.twarql.models.AnnotatedTweet;
import org.knoesis.tgd.database.MongoDBHandler;
import org.knoesis.tgd.utils.GetLatLongFromJson;

import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.Status;

public class LocationFetcher implements Extractor<Object>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private MongoDBHandler dbHandler = null;
	/**
	 * This constructor creates a dbhandler for every call/object.
	 * 
	 * TODO: Not the right way to do it and its a fix now. The 
	 * 		DBHandler can either be used outside the class or think of a better s
	 * 		way to do it.
	 *
	 */
	public LocationFetcher() {
		dbHandler = new MongoDBHandler();
	}
	@Override
	public Set<String> extract(Object tweet) {
		return null;
	}

	@Override
	public void process(AnnotatedTweet tweet) {
		Status status = tweet.getStatusTweet();
		GeoLocation location = status.getGeoLocation();

		// Collecting Tweet Content to get eventID from the tweet
//		String tweettext = status.getText();
//		String authorLocation = status.getUser().getLocation();
		// checkTimeConsumption("resetLatLong");
		/*
		 * Deal with tweet level geolocation. If the location is not null
		 * retrieve latitude and longitude. Else Query the location_latlong
		 * table to see if it has author location, if not decode the location.
		 */
		if (location != null) {
			tweet.getGeoLocation().setLatitude((float) location.getLatitude());
			tweet.getGeoLocation().setLongitude((float) location.getLongitude());
		}
		else {
//			ATweetGeoLocation dbGeoLocation = dbHandler.getGeoLocationData(tweet);
//			if(dbGeoLocation != null)
//				tweet.setGeoLocation(dbGeoLocation);
//			else
				tweet.setGeoLocation(decodeLocation(tweet));
		}

	}

	public ATweetGeoLocation decodeLocation(AnnotatedTweet aTweet) {
		ATweetGeoLocation geoLocation = new ATweetGeoLocation();
		Status status = aTweet.getStatusTweet();
		Place tweetPlace = status.getPlace();
		String profileLocation = status.getUser().getLocation();

		// location in user profile
		if (profileLocation != null && !profileLocation.isEmpty()) {
			String tweetPlaceFullName = null;

			// Tweet level location
			if (tweetPlace != null) {
				tweetPlaceFullName = tweetPlace.getFullName();
			}

			String url = GetLatLongFromJson
					.convertToURLSafe((tweetPlaceFullName != null && !tweetPlaceFullName.isEmpty()) ? tweetPlaceFullName
							: profileLocation);

			// Getting latitude and longitude of the tweet.
			float[] latLong = new float[2];
			latLong = GetLatLongFromJson.getCoordinates(url);

			geoLocation.setLatitude(latLong[0]);
			geoLocation.setLongitude(latLong[1]);

		}
		return geoLocation;
	}
}
