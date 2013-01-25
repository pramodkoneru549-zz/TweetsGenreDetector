package org.knoesis.twarql.models;
import java.io.Serializable;
import java.util.Set;


import twitter4j.Status;

/**
 * @author pavan
 * 
 * 
 * TODO: In this class for now I am adding another new object--STweet.
 * 		 This object contains all the metadata provided by twitter including the user metadata
 * 		 Also contains the json :).
 * 	     Need to debate on this design :D.
 * 
 * This class stores all the metadata of the status (tweet).
 * We are making it serializable because we want to pass it between
 * twitter storm bolts.
 *
 */
public class AnnotatedTweet implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private Status tweet; 
	private Set<String> entities;
	private String event;
	private ATweetGeoLocation geoLocation= new ATweetGeoLocation();
	
	// This is used to store the categories which we get using dbpedia SKOS
	// ontology.
	private Set<String> categories;
	
	//TODO: Pavan: I am not sure whether this is required.. Yet to be debated
	public AnnotatedTweet() {}

	//TODO Move this to factory
	public AnnotatedTweet(Status tweet) {
		this.setStatusTweet(tweet);
	}

	public void setEntities(Set<String> setEntity){
		entities=setEntity;
	}
	
	public Set<String> getEntities(){
		return entities;
	}
	

	@Override
	public String toString() {
		return "TWEET " +
		        "\n\tentities: "+getEntities();
//		        "\n\turls: "+getUrls();
	}

	public Status getStatusTweet() {
		return tweet;
	}

	public void setStatusTweet(Status tweet) {
		this.tweet = tweet;
	}

	public String getEvent() {
		return event;
	}

	public void setEvent(String event) {
		this.event = event;
	}

	public ATweetGeoLocation getGeoLocation() {
		return geoLocation;
	}

	public void setGeoLocation(ATweetGeoLocation geoLocation) {
		this.geoLocation = geoLocation;
	}

	public void setCategories(Set<String> categories) {
		this.categories = categories;
	}

	public Set<String> getCategories() {
		return categories;
	}
}
