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
public class Config {
	private static Log log = LogFactory.getLog(Config.class);
	private static Properties properties = new Properties();
	
	// Default Configuration
	private static String propertiesFile = GlobalConstants.DEFAULT_PROPERTIES_FILE;
	private static String rootFolder = GlobalConstants.DEFAULT_ROOT_FOLDER;
	
	
	public Config(String propertiesFile,String rootFolder){
		Config.propertiesFile = propertiesFile;
		Config.rootFolder = rootFolder;
	}
	
	/**
	 * This method is used to load the properties from the user
	 * defined propertied file.
	 */
	public static void load(){
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
	
	public static void main(String[] args) {
		Config.load();
		String keywords = Config.properties.getProperty("keywords");
		String twitterusername = Config.properties.getProperty("twitterusername");
		System.out.println("The keywords are "+ keywords);
		System.out.println("The username is "+ twitterusername);
		
	}
}
