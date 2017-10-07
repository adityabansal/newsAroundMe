$(function() {
  function StoriesViewModel() {
    var self = this;
    self.stories = ko.observableArray([]);
    self.lastRequest = null;
    self.lastNetworkRequest = null;
    self.isDataLoading = ko.observable(false)

    self.showStories = function(stories) {
      $.each(stories, function( index, story ) {
        self.stories.push(new window.StoryViewModel(story));
      });
    }

    self.loadStoriesInitialOfflineFirst = function(url) {
      // fetch fresh data
      var networkDataReceived = false;
      self.lastNetworkRequest = $.getJSON(url, function( stories ) {
        self.stories([]);
        self.showStories(stories);
        networkDataReceived = true;
        console.log("Updated stories from network for url: " + url);
      });

      if (!!caches) {
        // fetch cached data
        caches.match(url).then(function(response) {
          if (!response) throw Error("No data");
          return response.json();
        }).then(function(stories) {
          // don't overwrite newer network data
          if (!networkDataReceived) {
            self.stories([]);
            self.showStories(stories);
            console.log("Updated stories from cache for url: " + url);
          }
        }).catch(function() {
          // we didn't get cached data, the network is our last hope:
          return self.lastNetworkRequest;
        })
      }

      return self.lastNetworkRequest;
    }

    self.loadMoreStoriesFromNetwork = function(url) {
      return $.getJSON(url, function( stories ) {
        self.showStories(stories);
      });
    }

    self.loadStoriesInternal = function(url) {

      if(!!self.lastRequest && self.isDataLoading()) {
        self.lastRequest.abort();
      }

      self.isDataLoading(true);
      if (url.indexOf("skip") > 0 && url.indexOf("top") > 0) {
        self.lastRequest = self.loadMoreStoriesFromNetwork(url);
      } else {
        self.lastRequest = self.loadStoriesInitialOfflineFirst(url);
      }

      self.lastRequest.always(function() {
        self.isDataLoading(false);
      });

      self.lastRequest.then(function() {
        self.loadMoreStoriesAuto();
      })
    }

    self.loadData = function(url) {
      self.url = ko.observable(url);
      self.stories([]);

      self.loadStoriesInternal(url);
    }

    self.loadMoreStories = function() {
      var urlWithSkipAndTop = self.url() + "&skip=" + self.stories().length + "&top=5";

      self.loadStoriesInternal(urlWithSkipAndTop);
    }

    self.loadMoreStoriesAuto = function() {
      if (!self.isDataLoading()) {
        if (!!self.url && !!self.url() && self.stories().length < 11) {
          self.loadMoreStories();
        }
      }
    }
  }

  var vM = new StoriesViewModel();
  window.StoriesViewModel = vM;
  ko.applyBindings(vM, $("#storyList")[0]);
});