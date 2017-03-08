$(function() {
  function StoriesViewModel() {
    var self = this;
    self.stories = ko.observableArray([]);
    self.lastRequest = null;
    self.isDataLoading = ko.observable(false)

    self.loadStoriesInternal = function(url) {

      if(!!self.lastRequest && self.isDataLoading()) {
        self.lastRequest.abort();
      }

      self.isDataLoading(true);
      self.lastRequest = $.getJSON(url, function( stories ) {
        $.each( stories, function( index, story ) {
          self.stories.push(new window.StoryViewModel(story));
        });
        self.loadMoreStoriesIfAtEnd();
      });

      self.lastRequest.always(function() {
        self.isDataLoading(false);
      });
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

    self.loadMoreStoriesIfAtEnd = function() {
      if (($(window).height() > $(document).height()) || // space still there at end of page
          ($(document).height() - $(window).height() + 5 > $(window).scrollTop())) { // near the end of the document
        if (!self.isDataLoading()) {
          if (!!self.url && !!self.url()) {
            self.loadMoreStories();
          }
        }        
      }
    }

    // Load more data on page scroll
    $(window).scroll(function() {
      self.loadMoreStoriesIfAtEnd();
    });
  }

  var vM = new StoriesViewModel();
  window.StoriesViewModel = vM;
  ko.applyBindings(vM, $("#storyList")[0]);
});