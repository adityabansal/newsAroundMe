$(function() {
  function StoriesViewModel() {
    var self = this;
    self.stories = ko.observableArray([]);
    self.isDataLoading = ko.observable(false);

    self.loadData = function(url) {
      self.url = ko.observable(url);
      self.stories([]);

      self.isDataLoading(true);
      $.getJSON(url, function( stories ) {
        $.each( stories, function( index, story ) {
          self.stories.push(new window.StoryViewModel(story));
        });
        self.isDataLoading(false);
      });
    }

    self.loadMoreStories = function() {
      var urlWithSkipAndTop = self.url() + "&skip=" + self.stories().length + "&top=5";

      self.isDataLoading(true);
      $.getJSON(urlWithSkipAndTop, function(stories) {
        $.each( stories, function( index, story ) {
          self.stories.push(new StoryViewModel(story));
        });
        self.isDataLoading(false);
      });
    }

    // Load more data on page scroll
    $(window).scroll(function() {

      // End of the document reached?
      if ($(document).height() - $(window).height() == $(window).scrollTop()) {
        if (!self.isDataLoading()) {
          if (!!self.url && !!self.url()) {
            self.loadMoreStories();
          }
        }        
      }
    });
  }

  var vM = new StoriesViewModel();
  window.StoriesViewModel = vM;
  ko.applyBindings(vM, $("#storyList")[0]);
});