package com.streamjunction.topology.pictures;

import java.util.ArrayList;
import java.util.List;

/**
 * This object represents a cluster of images (array of images
 * located close to each other)
 * 
 *
 */
public class Cluster {

	private List<Marker> cluster = new ArrayList<Marker>();
	
	private double sumLat = 0;
	
	private double sumLon = 0;

	public Cluster(final List<Marker> cluster) {
		for (Marker m : cluster) {
			add(m);
		}
	}
	
	public boolean contains(Marker marker) {
		return cluster.contains(marker);
	}

	public List<Marker> getCluster() {
		return cluster;
	}
	/**
	 * Return a copy of the cluster without the expired images 
	 * @return
	 */
	public Cluster cleanup() {
		List<Marker> newList = new ArrayList<Marker>();
		for (Marker m : cluster) {
			if (!m.isExpired()) {
				newList.add(m);
			}
		}
		return new Cluster(newList);
	}
	
	public boolean isEmpty() {
		return cluster.isEmpty();
	}
	
	public void add(Marker m) {
		sumLat += m.getLatitude();
		sumLon += m.getLongitude();
		cluster.add(m);
	}
	
	/**
	 * Get center of the cluster (average X, average Y)
	 * @return
	 */
	public Marker getCenter() {
		if (cluster.size() > 0) {
			return new Marker(sumLat/cluster.size(), sumLon/cluster.size(), 0, null);
		} else {
			return null;
		}
	}
}
