//
// Main routing module
//
// The following page hierarchy is assumed:
// 
// * / - root; doesn't actually do anything, just a redirect to the landing page
// * /login - login page
// * /logout - logout action: destroy session and redirect to /
// * /static/* - static resources (css, img, js)
// * /pages/* - different pages for logged in users
// * /api/* - REST resources for logged in users
// * /forms/* - form submission endpoints for logged in users
// * /auth/* - authentication actions
// * /pp/* - TODO: publicly visible pages
// * /papi/* - TODO: read-only REST resources for publicly visible pages


//
// --------------------------------
//    IMPORT MODULES
// --------------------------------
//

var express = require("express")
  , compiless = require("express-compiless")
  , sjClient = require("./client.js")
  , noCache = require('connect-nocache')();

var web = express();

//
// --------------------------------
//    INITIALIZE SESSION STORAGE
// --------------------------------
//


// Server options

var port = process.env.PORT || 5000;
var site = process.env.SITE || 'http://localhost:'+port;

var drpcStream = process.env.SJ_STORM_STREAM_DRPC_URL;


//
// --------------------------------
//    INITIALIZE APPLICATION ROUTING
// --------------------------------
//

// ***
// Initialize middleware
// ***
web.set('views', __dirname + '/views');
web.set('view engine', 'jade');

// Set NODE_ENV env var to production or development
// to trigger the settings below
web.configure('development', function () {
    web.locals.pretty = true;
});

// General settings
web.configure(function() {        
    // initialize logging
    web.use(express.logger('dev'));
    // serve static content from /public
    // TODO: use CDN solution
    web.use('/static', compiless({ root: __dirname+'/public' }));
    // --> use { maxAge: oneDay } as the optional parameter to static to control caching
    web.use('/static', express.static(__dirname+'/public'));
    // parse cookies
    web.use(express.cookieParser());
    // parse body if application/x-www-form-urlencoded
    web.use(express.bodyParser());
    // route request
    web.use(web.router);
});


// ***
// Default redirect 
// ***
web.get('/', function (req, res) {
    res.redirect('/pages/home');
});

// ***
// Display pages
// ***

web.get('/pages/home', function (req, res) {
    res.render("main", {});
});

// ***
// Get the list of clusters
// ***
// The output of the call looks like the following:
//
// [ { location: { lat: ..., lng: ... }, images: [ ...url... ] } ]
web.get("/api/clusters", noCache, function (req, res) {
	 new sjClient.RESTClient(drpcStream).sendText('sorted', function (err, result) {		
		if (err) {
			console.log(err);
			res.send(500, "Internal error");
		} else {
      // the result of the invocation looks like this:
      // [ [ [
      //     [ { timestamp: ..., location: { ... }, images: url, locationAlias: ... } ]
      // ] ] ]
			//console.log(result);
      var clusters = [];
      if (result) {
        var r = JSON.parse(result);
        console.log("L1:"+r.length);
        if (r[0] && r[0][0]) {
          console.log("L2:"+r[0].length);
          var rr = r[0][0];
          console.log("L3:"+rr.length);
          for (var i=0; i<rr.length; i++) {
            var cluster = rr[i];
            console.log(cluster);
            if (cluster.length > 0) {
              console.log("cluster.length="+cluster.length);
              var location = cluster[0].location;
              var n = 1;
              var images = [ cluster[0].images ];
              for (var j=1; j<cluster.length; j++) {
                location.lat += cluster[j].location.lat;
                location.lng += cluster[j].location.lng;
                images.push(cluster[j].images);
                n ++;
              } 
              location.lat = location.lat / n;
              location.lng = location.lng / n;
              clusters.push({ location: location, images: images });
              console.log("clusters.length="+clusters.length);
            }
          }
        }
      }
      console.log("return clusters.length="+clusters.length);
			res.json(clusters);
		}	 
	 });
});
// ------------

// ------------

// ***
// Supporting services for the pages
// ***

// require('./api/web-api.js').api(web);

//*** START THE SERVER
//***********************

// *
// * 
// * Submitting the topology (add configuration)
// * 

sjClient.submit("storm/target/storm/storm", "storm/target/storm.jar",   
  "com.streamjunction.topology.pictures.topology.InstagramTridentTopology",
  process.env.SJ_SUBMIT_URL, 
  [ process.env.SJ_INSTAGRAM_CLIENT_ID ],
  function (err) {
    if (err) {
      console.log("Cannot submit topology: "+err);
    } 
    else {
      console.log('Successfully submitted topology, starting web server. ');
    }    
    
    web.listen(port, function() {
      console.log("Web listening on port: "+port);
    });
    
});



