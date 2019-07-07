$(function() {
  function StoriesViewModel() {
    var self = this;
    self.stories = ko.observableArray([]);
    self.dataLoader = new window.DataLoaderModel();
    self.isDataLoading = self.dataLoader.isDataLoading;

    self.appendStories = function(stories) {
      $.each(stories, function( index, story ) {
        self.stories.push(new window.StoryViewModel(story));
      });
    }

    self.showStories = function(stories) {
      self.stories([]);
      self.appendStories(stories)
    }

    self.loadStoriesInternal = function(url) {
      var lastRequest;

      if (url.indexOf("skip") > 0 && url.indexOf("top") > 0) {
        self.dataLoader.initialize(url, self.appendStories)
        lastRequest = self.dataLoader.loadDataFromNetwork();
      } else {
        self.dataLoader.initialize(url, self.showStories)
        lastRequest = self.dataLoader.loadDataOfflineFirst();
      }

      lastRequest.then(function() {
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