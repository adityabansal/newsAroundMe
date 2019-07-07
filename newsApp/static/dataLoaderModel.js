$(function() {
  function DataLoaderModel() {
    var self = this;
    self.url = "";
    self.loadDataCallback = null;
    self.lastNetworkRequest = null;
    self.isDataLoading = ko.observable(false);
    self.isLoadedFromCache = ko.observable(false);

    self.initialize = function(url, loadDataCallback) {
      self.url = url;
      self.loadDataCallback = loadDataCallback;    
    }

    self.loadDataOfflineFirst = function() {
      var url = self.url;

      // Cancel any pending requests.
      if(!!self.lastNetworkRequest && self.isDataLoading()) {
        self.lastNetworkRequest.abort();
      }

      self.isDataLoading(true);
      self.isLoadedFromCache(false);

      // Begin network request.
      var networkDataReceived = false;
      self.lastNetworkRequest = $.getJSON(url, function( response ) {
        self.loadDataCallback(response);
        self.isLoadedFromCache(false);
        networkDataReceived = true;
        console.log("Updated data from network for url: " + url);
      });

      // Fetch from cache if it's supported by browser.
      if ('caches' in window) {
        // fetch cached data
        caches.match(url).then(function(response) {
          if (!response) throw Error("No data");
          return response.json();
        }).then(function(responseJson) {
          // don't overwrite newer network data
          if (!networkDataReceived) {
            self.loadDataCallback(responseJson);
            self.isLoadedFromCache(true);
            console.log("Updated data from cache for url: " + url);
          }
        }).catch(function() {
          // we didn't get cached data, let the network request complete.
        })
      }

      self.lastNetworkRequest.always(function() {
        self.isDataLoading(false);
      });

      return self.lastNetworkRequest;
    }

    self.loadDataFromNetwork = function() {
      var url = self.url;

      // Cancel any pending requests.
      if(!!self.lastNetworkRequest && self.isDataLoading()) {
        self.lastNetworkRequest.abort();
      }

      self.isDataLoading(true);
      self.isLoadedFromCache(false);

      // Begin network request.
      self.lastNetworkRequest = $.getJSON(url, function( response ) {
        self.loadDataCallback(response);
        self.isLoadedFromCache(false);
        console.log("Updated data from network for url: " + url);
      });

      self.lastNetworkRequest.always(function() {
        self.isDataLoading(false);
      });

      return self.lastNetworkRequest;
    }
  }

  window.DataLoaderModel = DataLoaderModel;
});