package org.knoesis.tgd.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.knoesis.tgd.utils.GlobalConstants;


/**
 * This class is used to get all the configuration that is set
 * into config.properties file.
 * @author pramodkoneru549
 *
 */
public class ConfigManager {
	private static Log log = LogFactory.getLog(ConfigManager.class);
	private Properties properties = new Properties();
	
	public Properties getProperties() {
		return properties;
	}

	// Default Configuration
	private static String propertiesFile = GlobalConstants.DEFAULT_PROPERTIES_FILE;
	private static String rootFolder = GlobalConstants.DEFAULT_ROOT_FOLDER;
	
	/**
	 * This constructor loads the properties file from the given location and 
	 * file name.
	 * @param propertiesFile
	 * @param rootFolder
	 */
	public ConfigManager(String propertiesFile,String rootFolder){
		ConfigManager.propertiesFile = propertiesFile;
		ConfigManager.rootFolder = rootFolder;
		try {
			properties.load(new FileInputStream(rootFolder + "/" + propertiesFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			log.error("Properties file " + rootFolder + "/" + propertiesFile + "not Found.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * This constructor loads the properties from the default configs.
	 */
	public ConfigManager(){
		try {
			properties.load(new FileInputStream(rootFolder + "/" + propertiesFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			log.error("Properties file " + rootFolder + "/" + propertiesFile + "not Found.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static ConfigManager instance = null;
	
	public static ConfigManager getInstance(){
		if(instance == null){
			instance = new ConfigManager();
		}
		
		return instance;
	}
	
	/**
	 * Returns the set of events that has to be crawled by the crawler.
	 * 
	 * @return
	 */
	public Set<String> getEvents() {
		String events = properties.getProperty("events");
		return commaSeperatedToSet(events);
	}
	
	/**
	 * Returns the keywords for the particluar event from the configuration file
	 * 
	 * @param event
	 * @return
	 */
	public Set<String> getKeywordsForEvent(String event) {
		String keywords = properties.getProperty("event." + event.trim());
		return commaSeperatedToSet(keywords);
	}

	/**
	 * transforms the comma seperated words into a set
	 * 
	 * @param csw
	 * @return
	 */
	private Set<String> commaSeperatedToSet(String csw) {
		String[] wordArray = csw.split(",");
		Set<String> wordSet = new HashSet<String>();
		// trimming the elements of the array
		for (int i = 0; i < wordArray.length; i++) {
			wordSet.add(wordArray[i].trim());
		}
		return wordSet;
	}
	
	public boolean isClusterModeLocal(){
		boolean clusterMode = Boolean.parseBoolean(properties.getProperty("isClusterModeLocal"));
		return clusterMode;
	}
	
	public String getTwitterUsername(){
		return properties.getProperty("twitterusername");
	}
	
	public String getTwitterPassword(){
		return properties.getProperty("twitterpassword");
	}

	public static void main(String[] args) {
		ConfigManager manager = ConfigManager.getInstance();
		Properties props = manager.getProperties();
		String twitterusername = props.getProperty("twitterusername");
		System.out.println("Events " + manager.getEvents());
		System.out.println("The keywords are "+ manager.getKeywordsForEvent("Apple"));
		System.out.println("The username is "+ twitterusername);
		System.out.println("Mode is " + manager.isClusterModeLocal());
	}
}
