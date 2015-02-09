package com.streamjunction.topology.pictures.topology;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.streamjunction.sdk.client.CannotSubmitTopologyException;
import com.streamjunction.sdk.client.Submitter;
import com.streamjunction.sdk.log.Debug;
import com.streamjunction.topology.pictures.Cluster;
import com.streamjunction.topology.pictures.GeoCluster;
import com.streamjunction.topology.pictures.Marker;
import com.streamjunction.topology.pictures.spouts.PicturesFixedBatchSpout;

/**
 * A toplogy that accepts instagram markers as input and does clustering
 * depending on the geo-location
 * 
 */
public class InstagramTridentTopology {

	static final Logger logger = LoggerFactory
			.getLogger(InstagramTridentTopology.class);
	
	/**
	 * Inter cluster distance in meters
	 */
	public static final int INTER_CLUSTER_DISTANCE = 500;

	/**
	 * Maximum value of tuples that we will entertain. This is to prevent
	 * unbounded lists.
	 */
	static final int MAX_TUPLES = 1000;

	public static void main(String... args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		
		if (args.length < 2) {
			String client_id = args[0];

			// Local
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();

			cluster.submitTopology("instaGeoTopology", conf,
					buildTopology(client_id, drpc));

			for (int i = 0; i < 1000; i++) {
				Thread.sleep(5000);
			}
		} else {
			String submitUrl = args[0];
			String client_id = args[1];
			
			System.err.println("Submitting to " + submitUrl);
			conf.setNumWorkers(1);
			Properties inputMap = new Properties();
			inputMap.setProperty("InstaPictures", "storm-stream");
			try {
				Submitter.submitTopology(new URI(submitUrl), "InstaPictures",
						Submitter.getVersionFromResource("topology.version"),
						conf, inputMap, buildTopology(client_id, null));
			} catch (CannotSubmitTopologyException e) {
				System.err.println("Cannot submit topology: "
						+ e.getStatusCode() + " " + e.getReason());
				System.err.write(e.getBody());
				System.err.println();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}

	}

	public static StormTopology buildTopology(String client_id, final LocalDRPC drpc) {
		TridentTopology topology = new TridentTopology();

		/* spout */
		PicturesFixedBatchSpout instaSpout = 
				new PicturesFixedBatchSpout(
						client_id,
						new Fields("marker"), 
						500,
						10000,
						null);

		TridentState clusters = topology
				.newStream("instaSpout", instaSpout)
				.parallelismHint(1)
				.each(new Fields("marker"), new QuardrantLocator(),
						new Fields("quadrant"))
				.each(new Fields("quadrant", "marker"), new Debug("marker"))
				.groupBy(new Fields("quadrant")) // query the state by "quadrant"
				.persistentAggregate(new MemoryMapState.Factory(),
						new Fields("quadrant", "marker"),
						new ClusterReducerList(),
						new Fields("clusters")).parallelismHint(1);
		
		topology.newDRPCStream("storm-stream", drpc)
				.each(new Fields("args"), new IterateQuadrants(), new Fields("quadrant"))
				.each(new Fields("quadrant"), new Debug("emit-Q"))
				.groupBy(new Fields("quadrant"))
				.stateQuery(clusters, new Fields("quadrant"), new MapGet(),
						new Fields("clusters"))
				.each(new Fields("quadrant", "clusters"), new Debug("state-query"))
				.aggregate(new Fields("clusters"), new ClusterAggregator(), new Fields("result"))
				.each(new Fields("result"), new Debug("result"));

		return topology.build();
	}

	/**
	 * This function appends the Quardrant identifier at the end of the tuples.
	 * 
	 * @author srmore
	 * 
	 */
	@SuppressWarnings("serial")
	public static class QuardrantLocator extends BaseFunction {

		/* for Quadrant1 */
		public static double Q1_LATITUDE_MIN = 37.7704081;
		public static double Q1_LATITUDE_MAX = 37.819961;
		public static double Q1_LONGITUDE_MIN = -122.4762069;
		public static double Q1_LONGITUDE_MAX = -122.4137401;

		/* for Q2 */
		public static double Q2_LATITUDE_MIN = 37.7704081;
		public static double Q2_LATITUDE_MAX = 37.819961;
		public static double Q2_LONGITUDE_MIN = -122.4251326;
		public static double Q2_LONGITUDE_MAX = -122.3626658;

		/* for Q3 */
		public static double Q3_LATITUDE_MIN = 37.729848;
		public static double Q3_LATITUDE_MAX = 37.7794342;
		public static double Q3_LONGITUDE_MIN = -122.4250912;
		public static double Q3_LONGITUDE_MAX = -122.3626585;

		/* for Q4 */
		public static double Q4_LATITUDE_MIN = 37.72986452788;
		public static double Q4_LATITUDE_MAX = 37.77941772078;
		public static double Q4_LONGITUDE_MIN = -122.47617278071;
		public static double Q4_LONGITUDE_MAX = -122.41374009987;

		@Override
		public void execute(final TridentTuple tuple,
				final TridentCollector collector) {
			final Marker marker = (Marker) tuple.getValue(0);
			/* Check for the quardrents here */
			if (marker != null) {

				/* quardrant 1 */
				if ((marker.getLatitude() > Q1_LATITUDE_MIN)
						&& (marker.getLatitude() < Q1_LATITUDE_MAX)
						&& (marker.getLongitude() > Q1_LONGITUDE_MIN)
						&& (marker.getLongitude() < Q1_LONGITUDE_MAX)) {

					/* append the quardrent identifier at the end */
					collector.emit(new Values("Q1"));
				}

				/* quardrant 2 */
				else if ((marker.getLatitude() > Q2_LATITUDE_MIN)
						&& (marker.getLatitude() < Q2_LATITUDE_MAX)
						&& (marker.getLongitude() > Q2_LONGITUDE_MIN)
						&& (marker.getLongitude() < Q2_LONGITUDE_MAX)) {

					/* append the quardrent identifier at the end */
					collector.emit(new Values("Q2"));
				}

				/* quardrant 3 */
				else if ((marker.getLatitude() > Q3_LATITUDE_MIN)
						&& (marker.getLatitude() < Q3_LATITUDE_MAX)
						&& (marker.getLongitude() > Q3_LONGITUDE_MIN)
						&& (marker.getLongitude() < Q3_LONGITUDE_MAX)) {

					/* append the quardrent identifier at the end */
					collector.emit(new Values("Q3"));
				}
				/* quardrant 4 */
				else {
					/* no choice but Q4 */
					collector.emit(new Values("Q4"));
				}

			}

		}

		/**
		 * This function takes in the Marker and returns the Quardrant
		 */
		public static String returnQuardrant(final Marker marker) {
			/* quardrant 1 */
			if ((marker.getLatitude() > Q1_LATITUDE_MIN)
					&& (marker.getLatitude() < Q1_LATITUDE_MAX)
					&& (marker.getLongitude() > Q1_LONGITUDE_MIN)
					&& (marker.getLongitude() < Q1_LONGITUDE_MAX)) {

				/* append the quardrent identifier at the end */
				return "Q1";
			}

			/* quardrant 2 */
			else if ((marker.getLatitude() > Q2_LATITUDE_MIN)
					&& (marker.getLatitude() < Q2_LATITUDE_MAX)
					&& (marker.getLongitude() > Q2_LONGITUDE_MIN)
					&& (marker.getLongitude() < Q2_LONGITUDE_MAX)) {

				/* append the quardrent identifier at the end */
				return "Q2";
			}

			/* quardrant 3 */
			else if ((marker.getLatitude() > Q3_LATITUDE_MIN)
					&& (marker.getLatitude() < Q3_LATITUDE_MAX)
					&& (marker.getLongitude() > Q3_LONGITUDE_MIN)
					&& (marker.getLongitude() < Q3_LONGITUDE_MAX)) {

				/* append the quardrent identifier at the end */
				return "Q3";
			}
			/* quardrant 4 */
			else {
				/* no choice but Q4 */
				return "Q4";
			}
		}

	}
	
	public static class IterateQuadrants implements Function {

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void cleanup() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			for (String q : Arrays.asList("Q1", "Q2", "Q3", "Q4")) {
				collector.emit(new Values(q));
			}
		}
		
	}

	@SuppressWarnings("serial")
	public static class ClusterAggregator extends BaseAggregator<List<List<Marker>>> {

		@Override
		public List<List<Marker>> init(Object batchId, TridentCollector collector) {
			return new ArrayList<List<Marker>>();
		}

		@Override
		public void aggregate(List<List<Marker>> val, TridentTuple tuple,
				TridentCollector collector) {

			List<Cluster> clusters = (List<Cluster>) tuple.getValueByField("clusters");
			if (clusters != null) {
				for (Cluster c : clusters) {
					val.add(c.getCluster());
				}
			}

		}

		@Override
		public void complete(List<List<Marker>> val, TridentCollector collector) {

			collector.emit(new Values(val));
		}

	}

	/**
	 * Used to store the lists in memory for DRPC.
	 * 
	 */
	@SuppressWarnings("serial")
	public static class ClusterReducerList implements
			ReducerAggregator<List<Cluster>> {

		@Override
		public List<Cluster> init() {
			return new ArrayList<Cluster>();
		}

		@SuppressWarnings("unchecked")
		@Override
		public List<Cluster> reduce(List<Cluster> clusters,
				TridentTuple tuple) {

			if (tuple == null || tuple.isEmpty()) {
				return clusters;
			}
			
			Marker marker = (Marker) tuple.getValueByField("marker");
			
			Debug.log("Reducing marker: "+marker);
			
			List<Cluster> newState = new ArrayList<Cluster>();

			boolean added = false;
			
			boolean skip = false;
			
			/* Only add clusters we have not seen before */
			for(Cluster c: clusters)
			{
				if (!c.isEmpty()) {
					if (!added) {				
						Marker center = c.getCenter();
						if (center != null) {
							if (marker.distanceTo(center) < GeoCluster.MAX_INNER_CLUSTER_DISTANCE) {
//								Debug.log("Adding marker to cluster");
								Cluster newCluster = c.cleanup();
//								Debug.log("Cluster after cleanup: "+newCluster);
								if (!newCluster.contains(marker)) {
									Debug.log("Adding marker to cluster");
									newCluster.add(marker);
									newState.add(newCluster);
									added = true;
								} else {
									skip = true;
								}
							} else {
								newState.add(c);
							}
						} else {
							newState.add(c);
						}
					} else {
						newState.add(c);
					}
				}
			}
			
			if (!added && !skip) {
				newState.add(new Cluster(Arrays.asList(marker)));
			}
		
			Debug.log("Updated state: "+newState);
			
			return newState;			
		}
	}
	
}
