package com.streamjunction.topology.pictures.spouts;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.streamjunction.sdk.log.Debug;
import com.streamjunction.topology.pictures.Marker;

/**
 * The spout monitors the Instagram API and emits new images 
 * for the given location (SF)
 * 
 *
 */
@SuppressWarnings("serial")
public class PicturesFixedBatchSpout implements IBatchSpout {
	
	/* Max number of records returned from instagram */
	private static final int MAX_COUNT = 5000;
	
	/* next_min_id for pagination */
	private transient String next_min_id;
	
	private transient BlockingQueue<Marker> queue;
		
	private transient Map<Long, List<Marker>> batches;
	
	private transient ScheduledExecutorService scheduledExecutorService;
	
	private transient volatile String testStr; 
	
	private String client_id;
	
	/* Let the topology declare fields*/
	private Fields fields;
	
	private long markerExpiration = 5*3600*1000; /* ms */
	
	/* batch size */
	private int maxBatchSize;
	
	private int queueSize;
	
	private String testData; 
	
	public PicturesFixedBatchSpout(
			final String client_id,
			final Fields fields, 
			final int maxBatchSize,
			final int queueSize,
			final String testData)
	{
		this.fields = fields;
		this.maxBatchSize = maxBatchSize;
		this.queueSize = queueSize;
		this.testData = testData;
		this.client_id = client_id;
	}
	
	public PicturesFixedBatchSpout(String client_id)
	{
		this(client_id, new Fields("marker"), 500, 10000, null);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context) {
		
		queue = new ArrayBlockingQueue<Marker>(queueSize);
		
		batches =  new HashMap<Long, List<Marker>>();
		
		testStr = testData;
		
		//circularQueue = new CircularFifoQueue<Marker>(5000);
		scheduledExecutorService = Executors
				.newScheduledThreadPool(1);		
		
		//queue = new LinkedBlockingQueue<Marker>(5000);
		scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
					
					List<Marker> markers = new ArrayList<Marker>();
					
					List<Marker> toAdd; 
					
					while((toAdd = getData()).size() > 0 && markers.size() < queueSize)
					{
						//System.out.println("Spout: Get more "+markerList.size());
						markers.addAll(toAdd);
					}
					
					if (!markers.isEmpty()) {
						Debug.log("adding markers to the queue: "+markers);
					}
					
					synchronized (PicturesFixedBatchSpout.this.queue) {
						for (Marker m : markers) {
							if (!queue.offer(m)) {
								queue.poll();
								queue.offer(m);
							}
						}
						if (!markers.isEmpty()) {
							Debug.log("queue after adding markers: "+queue);
						}
					}
					
			}
		}, 1, 30, TimeUnit.SECONDS);
				
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		
//		Debug.log("emitBatch");
		
		List<Marker> batch = batches.get(batchId);
		
		if(batch == null)
		{
			batch = new ArrayList<Marker>();
			
			synchronized (queue) {				
				queue.drainTo(batch, maxBatchSize);				
			}
			
			if (!batch.isEmpty()) {
				batches.put(batchId, batch);
			}			
		}
		
		
		//System.out.println("Emitting "+batch.size());
		
		if (!batch.isEmpty()) {
			Debug.log(String.format("Emitting batch: batchId=%d batch=%s", batchId, batch));
		}
		
		for(Marker marker : batch)
		{
			collector.emit(new Values(marker));
		}		
	}

	@Override
	public void ack(long batchId) {
		this.batches.remove(batchId);
	}

	@Override
	public void close() {
		scheduledExecutorService.shutdownNow();
		
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map getComponentConfiguration() {
		Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
	}

	@Override
	public Fields getOutputFields() {
		return fields;
	}
	
	
	
	/**
	 * Get the data from Instagram
	 * @return
	 */
	private List<Marker> getData() {		
		
		Debug.log(String.format("getData (testStr=%s)", testStr));
		
		HttpURLConnection conn = null;
		List<Marker> markerList = new ArrayList<Marker>();
		String urlAddress = "https://api.instagram.com/v1/geographies/8597546/media/recent?client_id="+client_id /*&COUNT="+MAX_COUNT*/;
		
		/* pagination */
		if(next_min_id != null)
		{				
			/* get 50 recent updates, currently it returns only 30 max */
			urlAddress += "&MIN_ID="+next_min_id;
		}		
		
		try {

			if (testStr == null) {
				URL url = new URL(urlAddress);
	
				/* if behind proxy */
							
				conn = (HttpURLConnection) url.openConnection();
				conn.setRequestMethod("GET");
				conn.setRequestProperty("Accept", "application/json");
				conn.setRequestProperty("cache-control", "max-age=0");
	
				if (conn.getResponseCode() != 200) {
					throw new RuntimeException("Failed : HTTP error code : "
							+ conn.getResponseCode());
				}
	
				BufferedReader br = new BufferedReader(new InputStreamReader(
						(conn.getInputStream())));

				/* do the parsing business */
				final JSONParser parser = new JSONParser();
	
				Object obj = parser.parse(br.readLine());
				JSONObject mainJson = (JSONObject) obj;
				
				JSONObject pagination = (JSONObject) mainJson.get("pagination");
				if(pagination != null)
				{
					String new_next_min_id = (String) pagination.get("next_min_id");
					if (new_next_min_id != null) {
						if (new_next_min_id.equals(next_min_id)) {
							return Collections.emptyList();
						} else {
							next_min_id = new_next_min_id;
						}
					}
					//System.out.println("Next MIN ID : "+next_min_id);
				}
				
				JSONArray dataArray = (JSONArray) mainJson.get("data");
	
				for (Iterator<JSONObject> itr = dataArray.iterator(); itr.hasNext();) {
					
					JSONObject dataObj = itr.next();
	
					Debug.log(String.format("Parsing: %s", dataObj));
					
					JSONObject dataLocation = (JSONObject) dataObj.get("location");
	
					final double latitude = (Double) dataLocation.get("latitude");
					final double longitude = (Double) dataLocation.get("longitude");
					final String locationAlias = (String) dataLocation.get("name");
	
					final String ts = (String) dataObj.get("created_time");
					final long timestamp = Long.parseLong(ts) * 1000 + markerExpiration; /* ms */
					final String link = (String) dataObj.get("link");
	
					Marker marker = new Marker(latitude, longitude, timestamp,
							link, locationAlias);
					// queue.offer(marker);
					if(!markerList.contains(marker))
					{
						markerList.add(marker);
					}
					
					/*
					if(!circularQueue.contains(marker))
					{
						circularQueue.add(marker);
						markerList.add(marker);
					}
					*/
				}
				
			} else {
//				Debug.log(String.format("getData:parse-test-data=%s", testStr));
				//System.out.println(String.format("getData:parse-test-data=%s", testStr));
				JSONArray arr = (JSONArray) new JSONParser().parse(testStr);
				for (Object o : arr) {
					markerList.add(Marker.fromJSON(((JSONObject)o)));
				}
//				Debug.log(String.format("getData:markers=%s", markerList));
				testStr = "[]";
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
		return markerList;
	}

}
