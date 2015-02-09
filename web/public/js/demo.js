var testApp = angular.module("instagramDemo", [ 
  "AngularGM", "ui.slider"
]);


testApp.controller("InstagramDemoController", ["$scope", "$location", "$http",
  function ($scope, $location, $http) {
    
    $scope.triggerOpenInfoWindow = function(marker) {
      $scope.markerEvents = [
        {
          event: 'openinfowindow',
          ids: [marker.id]
        },
      ];
    }
    
    $scope.options = {
      map: {
        center: new google.maps.LatLng(37.783, -122.417),
        zoom: 11,
        mapTypeId: google.maps.MapTypeId.ROADMAP
      },
    };
    
    $scope.lines = function (cluster) {
      
      if (!cluster) {
        return [];
      }
      
      var l = Math.floor(cluster.images.length / 4);
      
      if (cluster.images.length % 4 > 0) {
        l += 1;
      }
      
      return range(l);
      
    };
    
    $scope.columns = function (i, cluster) {
      
      if (!cluster) {
        return [];
      }

      var lines = $scope.lines(cluster).length;
      
      if (i < lines - 1) {
        return range(4);
      } else {
        if (cluster.images.length % 4 == 0) {
          return range(4);
        } else {
          return range(cluster.images.length % 4);
        }
      }
            
    };
    
    $scope.image = function (i, j, cluster) {
      
      return cluster.images[i*4+j];
      
    };
    
    $scope.onOpenInfoWindow = function (object, marker) {
      $scope.selectedCluster = object; 
      $scope.infoWindow.open(marker.getMap(), marker)
    }
    
    var range = function (r) {
      
      var res = [];
      
      for (var i=0; i<r; i++) {
        res.push(i);
      }
      
      return res;
      
    }
    
    var generateImageArray = function () {
      
      var l = Math.random() * 12;
      
      var res = [];
      
      for (var i = 0; i<l; i++) {
        res.push("img-"+i+".png");
      }
      
      return res;
    };
    
    $scope.markers = [];
    /*
    $scope.markers = function() {
    	$http({
          url: "/pages/clusters",
          method: "GET"
        }).success(function (data) {
          $scope.markers = data;
        }).error(function () {
          $scope.markers = true;
        });
    
    }
    */
    
    var loadData = function (cb) {
      
      $http.get("/api/clusters").
        success(function (data) {
          cb(data);
        });
      /*
      var data = [];
      for (var i = 0; i<100; i++) {
        data.push({          
          location : {
            lat : $scope.options.map.center.lat() + Math.random()*0.2 - 0.1,
            lng : $scope.options.map.center.lng() + Math.random()*0.2 - 0.1
          },
          images: generateImageArray()
        });
      }
      cb(data);
      */
      /*
      var data = [];
      $http.get('/pages/clusters').
      		success(function(data, status, headers, config) {
    			console.log("Success !");
    			console.log(data);
    			data = angular.fromJson(data);
    			//cb(data);
  			}).
  			error(function(data, status, headers, config) {
    			console.log("FAILURE !");
    			//cb(data);
  			});
  		*/
      
      
    };
        
    var refresh = function() {
      loadData(function (clusters) {
        // this callback must be invoked inside the tracking context of the scope
        // callback from $http is OK
        var markers = [];
        var minSize = $scope.minSize || 1;
        for (var i = 0; i<clusters.length; i++) {
          if (clusters[i].images && clusters[i].images.length >= minSize) {
            markers.push({
              id : i,
              location : clusters[i].location,
              images : clusters[i].images
            });
          }
        }      
        $scope.markers = markers;
      });
    };

    $scope.$watch("minSize", function() {
      refresh();
    });  

    refresh();
  }]);

