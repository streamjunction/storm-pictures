package com.streamjunction.topology.pictures;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * A number of utility functions
 * 
 * 
 *
 */
public class GeoCluster {

	/** Distance in meters -- maximum cluster radius */
	public static int MAX_INNER_CLUSTER_DISTANCE = 500;

	/**
	 * Return distance in meters between 2 points
	 * 
	 * @param lat1
	 * @param lat2
	 * @param lon1
	 * @param lon2
	 * @return
	 */
	public static double distance(double lat1, double lon1, double lat2, double lon2) {

	    final double R = 6371; // Radius of the earth

	    Double latDistance = deg2rad(lat2 - lat1);
	    Double lonDistance = deg2rad(lon2 - lon1);
	    Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
	            + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2))
	            * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
	    Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
	    double distance = R * c * 1000; // convert to meters

	    return distance;
	}

	private static double deg2rad(double deg) {
	    return (deg * Math.PI / 180.0);
	}

	public static void main(String... strings) throws IOException,
			ParseException {
		
		doTest();
	}

	private static void doTest() throws IOException, ParseException {
		final List<Marker> markerList = new ArrayList<Marker>();
		URL url = new URL(
				"https://api.instagram.com/v1/media/search?lat=37.7749295&lng=-122.4194155&client_id=62ab2382567c4fe7b73a6c32e5e18a67&distance=1000");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");

		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ conn.getResponseCode());
		}

		BufferedReader br = new BufferedReader(new InputStreamReader(
				(conn.getInputStream())));

		String output;

		System.out.println("Output from Server .... \n");

		/*
		 * while ((output = br.readLine()) != null) {
		 * System.out.println(output); }
		 */

		/* do the parsing business */
		final JSONParser parser = new JSONParser();

		Object obj = parser.parse(br.readLine());
		JSONObject mainJson = (JSONObject) obj;

		JSONArray dataArray = (JSONArray) mainJson.get("data");

		for (Iterator<JSONObject> itr = dataArray.iterator(); itr.hasNext();) {
			JSONObject dataObj = itr.next();
			// System.out.println(dataObj);

			JSONObject dataLocation = (JSONObject) dataObj.get("location");
			// System.out.println(dataLocation);

			final double latitude = (Double) dataLocation.get("latitude");
			final double longitude = (Double) dataLocation.get("longitude");
			final String locationAlias = (String) dataLocation.get("name");

			final String ts = (String) dataObj.get("created_time");
			final long timestamp = Long.parseLong(ts);
			final String link = (String) dataObj.get("link");

			Marker marker = new Marker(latitude, longitude, timestamp, link,
					locationAlias);
			// queue.offer(marker);
			markerList.add(marker);
		}

		System.out.println("Marker list size: " + markerList.size());
		// System.out.println(mainJson.get("data"));

		conn.disconnect();

		// TODO add test
	}

}
