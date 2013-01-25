package org.knoesis.twarql.models;

/**
 * This class is used to store the location information of a tweet.
 * @author pavan
 *
 */
public class ATweetGeoLocation {
	private float latitude;
	private float longitude;
	public static float NO_LATITUDE_LONGITUDE = 10000;
	
	public ATweetGeoLocation() {
		this.setLatitude(NO_LATITUDE_LONGITUDE);
		this.setLongitude(NO_LATITUDE_LONGITUDE);
	}
	
	public float getLatitude() {
		return latitude;
	}
	public void setLatitude(float latitude) {
		this.latitude = latitude;
	}
	public float getLongitude() {
		return longitude;
	}
	public void setLongitude(float longitude) {
		this.longitude = longitude;
	}
}
