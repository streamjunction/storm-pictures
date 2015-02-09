package com.streamjunction.topology.pictures;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * The representation of the picture with coordinates
 * 
 *
 */
@SuppressWarnings("serial")
public class Marker implements Serializable {
	private double latitude;
	private double longitude;
	private long expiration;
	private String locationAlias;
	private String link;

	public Marker(final double latitude, final double longitude,
			final long expiration, final String link) {

		this(latitude, longitude, expiration, link, null);
	}

	public Marker(final double latitude, final double longitude,
			final long expiration, final String link, final String locationAlias) {
		this.latitude = latitude;
		this.longitude = longitude;
		this.expiration = expiration;
		this.link = link;
		this.locationAlias = locationAlias;
	}

	/* standard drill */
	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 31).append(link).toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Marker)) {
			return false;
		}
		if (obj == this) {
			return true;
		}

		Marker rhs = (Marker) obj;

		return new EqualsBuilder().append(link, rhs.link)
				.isEquals();
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public long getExpiration() {
		return expiration;
	}

	public void setExpiration(long expiration) {
		this.expiration = expiration;
	}

	public String getLocationAlias() {
		return locationAlias;
	}

	public void setLocationAlias(String locationAlias) {
		this.locationAlias = locationAlias;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	@SuppressWarnings("unchecked")
	@Override
	public String toString() {
		/* need a proper json value */
		JSONObject marker = new JSONObject();
		JSONObject loc = new JSONObject();
		
		loc.put("lat", new Double(latitude));
		loc.put("lng", new Double(longitude));
		
		marker.put("location", loc);		
		marker.put("images", link);
		marker.put("expiration", new Long(expiration));
		marker.put("locationAlias", locationAlias);
		StringWriter out = new StringWriter();
		try {
			marker.writeJSONString(out);
			return out.toString();
		} catch (IOException e) {			
			new RuntimeException(e);
		}
		
		/* we failed */
		return "Latitude: " + Double.toString(latitude) + " Longitude: "
		+ Double.toString(longitude) + " Expiration: "
		+ Double.toString(expiration) + " Link: " + link;
	}
	
	public static Marker fromJSON(JSONObject json) {
		JSONObject loc = (JSONObject) json.get("location");
		return new Marker(((Number)loc.get("lat")).doubleValue(), ((Number)loc.get("lng")).doubleValue(),
				((Number)json.get("expiration")).longValue(), (String) json.get("images"), (String) json.get("locationAlias"));
	}
	
	public static Marker fromString(String str) throws ParseException {		
		JSONObject json = (JSONObject) new JSONParser().parse(str);
		return fromJSON(json); 
	}
	
	public double distanceTo(Marker m) {
		return GeoCluster.distance(latitude, longitude, m.getLatitude(), m.getLongitude());
	}
	
	public boolean isExpired() {
		return expiration <= System.currentTimeMillis();
	}

}
