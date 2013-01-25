package org.knoesis.tgd.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

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
	
	private static ConfigManager getInstance(){
		if(instance == null){
			instance = new ConfigManager();
		}
		
		return instance;
	}
	

	public static void main(String[] args) {
		Properties props = ConfigManager.getInstance().getProperties();
		String keywords = props.getProperty("keywords");
		String twitterusername = props.getProperty("twitterusername");
		System.out.println("The keywords are "+ keywords);
		System.out.println("The username is "+ twitterusername);
		
	}
}
