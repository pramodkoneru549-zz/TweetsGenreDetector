package org.knoesis.tgd.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URLEncoder;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
/**
 * 
 * @author wenbo
 * This class is used to get the location co-ordinates of the 
 * place. It uses google API to get the JSON and extracts co-ordinates
 * from that JSON
 *
 */
public class GetLatLongFromJson {

	public static float NO_LATITUDE_LONGITUDE = 10000;

	private static String convertStreamToString(InputStream is) {
		/*
		 * To convert the InputStream to String we use the
		 * BufferedReader.readLine() method. We iterate until the BufferedReader
		 * return null which means there's no more data to read. Each line will
		 * appended to a StringBuilder and returned as String.
		 */
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		StringBuilder sb = new StringBuilder();

		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				sb.append(line + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sb.toString();
	}

	/*
	 * This is a test function which will connects to a given rest service and
	 * prints it's response to Android Log with labels "Praeda".
	 */
	public static float[] getCoordinates(String url) {
		HttpClient httpclient = new DefaultHttpClient();
		// Prepare a request object
		HttpGet httpget = new HttpGet(url);
		float[] coordinates_array = new float[2];
		// Execute the request
		HttpResponse response;
		String ifExceptionThenPrint = "";
		try {
			response = httpclient.execute(httpget);
			// Get hold of the response entity
			HttpEntity entity = response.getEntity();
			// If the response does not enclose an entity, there is no need
			// to worry about connection release

			if (entity != null) {
				// A Simple JSON Response Read
				InputStream instream = entity.getContent();
				String result = convertStreamToString(instream);
				//System.out.println(result);
				ifExceptionThenPrint = entity.getContentLength() + "\n" + result;
				// A Simple JSONObject Creation
				JSONObject json = new JSONObject(result);

				// A Simple JSONObject Parsing
				if (json.has("Placemark")) {
					JSONArray nameArray = (JSONArray) json.get("Placemark");
					JSONObject point = (JSONObject) nameArray.getJSONObject(0)
					.get("Point");

					JSONArray coordinates = (JSONArray) point
					.get("coordinates");
					coordinates_array[0] = (float) coordinates.getDouble(0);
					coordinates_array[1] = (float) coordinates.getDouble(1);
				}
				else {
					coordinates_array[0] = NO_LATITUDE_LONGITUDE;
					coordinates_array[1] = NO_LATITUDE_LONGITUDE;
				}

				// Closing the input stream will trigger connection release
				instream.close();
				return coordinates_array;
			}

		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			System.out.println(ifExceptionThenPrint);
			e.printStackTrace();
		}
		return coordinates_array;

	}

	/**
	 * Encodes the URL into UTF-8
	 * @param place
	 * @return
	 */
	public static String convertToURLSafe(String place) {
		String encode = null;
		try {
			encode = URLEncoder.encode("\"" + place + "\"","UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		String url = "http://130.108.28.206/proxy/googleWrapper.php?location="
			+ encode;
		return url;
	}

	public static void main(String args[]) throws MalformedURLException,
	URISyntaxException {

		float[] coordinates = new float[3];

		coordinates = getCoordinates(convertToURLSafe("Woodside, New York"));
		System.out.println(coordinates[0]);
		System.out.println(coordinates[1]);
	}

}