Summary
=======

This example application demonstrates integration with third-party APIs. The idea
of the app is that it monitors Instagram, periodically pulling form it the 
fresh pictures for the given area. The picture entries (coordinates, timestamp
and image URL) are then fed to the topology which arranges them into clusters
500 meters max in radius. We only keep every particular picture for a limited
time (5 hours by default). The Instagram entries are obtained by the custom
spout, while the data query (the current list of the clusters) is obtained 
via a DRPC call. To store the state the Trident memory map is used.

The web part of the app displays the data on the map.

Building
========

1. Clone the sample repository: `git clone https://github.com/streamjunction/storm-pictures.git`
2. Create heroku application from it: `heroku apps:create`
3. Register with https://streamjunction.com
4. Create a project at http://streamjunction.com . For this project 
   configure 1 streams: `storm-stream`. It must be a 
   DRPC stream
5. You will need 2 URLs from your StreamJunction project: topology submit URL, 
   "storm-stream" stream URL. You can obtain all these URLs
   at your project page
6. Configure the following heroku properties: `SJ_SUBMIT_URL` - topology submit
   URL, `SJ_STORM_STREAM_DRPC_URL` - "storm-stream" stream URL
7. Subscribe your application for Redis Cloud instance (it should result
   in the new property in the config: `REDISCLOUD_URL`). Redis is used to coordinate
   Instagram spouts (in case you have more than one spout instance)
8. Subscribe for Instagram API and put the Client ID in the heroku configuration
   property `SJ_INSTAGRAM_CLIENT_ID`
9. Set the multilang buildpack for the project: `heroku config:set BUILDPACK_URL=https://github.com/ddollar/heroku-buildpack-multi.git`
10. Submit the project to heroku: `git push heroku master`
11. Open your project page at herokuapp.com. You should be able to see the
    map with markers corresponding to the clusters of pictures. Since we 
    use memory map store for the state, the state starts empty on every 
    topology restart and then slowly gets the new updates from Instagram.

Debugging
=========

The Streamjunction SDK includes several convenient tools those let you 
reconfigure the topology behavior on the fly. The most important one is 
runtime properties. The value of the property can be entered through the
Streamjunction UI in the project configuration. It will be stored encrypted
in the Streamjunction database, so it is a natural place to keep credentials.
The following SDK method can be used to access the value of the property:
`com.streamjunction.sdk.RuntimeProperty.getProperty(String)`. It will cache
the value, but it will refresh it every few seconds, so you can get almost 
instant response to the changes you make in the UI.

This becomes useful when using debug print. The Trident filter 
`com.streamjunction.sdk.log.Debug` and its static method `log` can be used
to put the debug information in the worker log. However they will only
trigger if you have the runtime property `sjdebug` set to `true`.

The logs can be streamed to the client machine. Right now there is no 
dedicated command line tool for that, but one can use `wget` to get 
the log stream:

    $ wget -q -t 0 --output-document - {URL}

Where `{URL}` has the following syntax:

    https://{AUTH}@jets.streamjunction.com:8443/logs/topology/{PROJECT}/{TOPO}?tag={TAG}

Here:

- `{AUTH}` is `{KEY}:{SECRET}` - the topology key and secret - you can get them
  in the Streamjunction UI. These are the same key and secret used to authenticate
  when deploying the topology
- `{PROJECT}` is the name of the project 
- `{TOPO}` is the name of the topology
- `{TAG}` is an arbitrary string identifying the log consumer. If for some reason
  your stream is disconnected, you can restart it from the same place you left
  if you supply the same tag 

Also note that logs with regex search are available in the Streamjunction UI
in the topology management tab.




