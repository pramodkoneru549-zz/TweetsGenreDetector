package org.knoesis.tgd.utils;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.knoesis.tgd.config.ConfigManager;
/**
 * 
 * This class constructs the Lists and Maps required for 
 * event and keywords processing. This class is used to
 *  
 * 1. Add keywords into the Twitter Streaming API
 * 2. Keywords<-->Event Mapping for detecting events for streamed tweets 
 * @author pramod, wenbo
 * @author pavan -- Just added the properties file reader 
 *
 */
public class EventIdKeywordsGenerator {
	private static Log log = LogFactory.getLog(EventIdKeywordsGenerator.class);
	private static Map<String,Set<Set<String>>> keywordsMap = new HashMap<String, Set<Set<String>>>();
	private static Map<Set<String>,String> keywordEventMap = new HashMap<Set<String>, String>();
	private static ArrayList<String> totalkeywords = new ArrayList<String>();
	private ConfigManager config;

	/**
	 * Provide the properties file 
	 * @param propFile
	 */
	public EventIdKeywordsGenerator(ConfigManager config) {
		log.info("Initializing the LOG File");
		this.config = config;
		createFromProperties();
	}

	/**
	 * Method generates data for appropriate datastructures
	 * Map -- Event Keyword
	 * Inverse Map -- Keyword Event
	 * List -- All keywords 
	 * @author koneru, wenbo, pavan
	 */
	private void createFromProperties() {
		Set<String> events = config.getEvents();
		log.info("Events included: "+events.toString());
		for(String event: events){
			Set<String> keywords = null;
			try {
				keywords = config.getKeywordsForEvent(event);
			} catch (NullPointerException e) {
				log.error("No keywords for this event: " + event);
				log.error("Please enter the keywords into config file");
			}
			// Adding all the keywords to feed to Twitter Streaming API
			if(keywords != null)
				totalkeywords.addAll(keywords);

			// Iterating through the keywords to create a keywords Hashmap
			if(keywords != null)
				for (String keyword : keywords) {
					String[] keys = keyword.trim().split(" +");

					// Converting array into list
					List<String> list = Arrays.asList(keys);

					// Converting list to HashSet.
					Set<String> keysSet = new HashSet<String>(list);

					Set<Set<String>> keyset = new HashSet<Set<String>>();

					if (keys.length > 1 ) {
						for (int i = 0; i < keys.length; i++) {						
							keyset.add(keysSet);

							// Checking if the Map already has the key.
							if(keywordsMap.containsKey(keys[i])){
								keywordsMap.get(keys[i]).add(keysSet);

							}else{
								keywordsMap.put(keys[i],keyset);
							}
						}
					}else {
						keyset.add(keysSet);
						keywordsMap.put(keys[0],keyset);

					}

					// Making a hashmap with keywords set and event.
					keywordEventMap.put(keysSet, event);
				}
		}

		log.debug("Keywords Map is : " + keywordsMap.toString());
		log.debug("KeywordEventMap is: "+keywordEventMap.toString());

	}

	public  Map<String,Set<Set<String>>> getKeywordsMap(){
		return keywordsMap;
	}

	public  Map<Set<String>,String> getkeywordEventMap(){
		return keywordEventMap;
	}

	public  ArrayList<String> getTotalKeywords(){
		return totalkeywords;
	}

	public static void main(String[] args) {
		ConfigManager configParams = ConfigManager.getInstance();
		EventIdKeywordsGenerator mapGenerator = new EventIdKeywordsGenerator(configParams);
		mapGenerator.createFromProperties();
		System.out.println(mapGenerator.getkeywordEventMap());
		System.out.println(mapGenerator.getKeywordsMap());
	}

}
