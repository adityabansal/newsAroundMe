$(function() {
  function StoryDetailViewModel() {
    var self = this;
    self.docId = null;
    self.articles = ko.observableArray([]);
    self.languageFilterVM = window.LanguageFilterViewModel;
    self.lastRequest = null;
    self.isDataLoading = ko.observable(false);

    self.loadStoryInternal = function(url) {

      if(!!self.lastRequest && self.isDataLoading()) {
        self.lastRequest.abort();
      }

      self.isDataLoading(true);
      self.articles([]);
      self.lastRequest = $.getJSON(url, function( response ) {
        self.articles([]);
        self.articles(response.articles.map(article =>
          new window.ArticleViewModel(article)));
        console.log("Updated story from network for url: " + url);
      });

      self.lastRequest.always(function() {
        self.isDataLoading(false);
      });
    }

    self.loadStory = function(docId) {
      self.docId = docId;
      var url = "/api/story/" + self.docId;

      self.loadStoryInternal(url)
    }

    self.languageFilterVM.languageFilter.subscribe(function(newValue) {
      var newUrl = "/api/story/" + self.docId;
      if (!!newValue) {
        newUrl = newUrl + "&" + newValue;
      }

      self.loadStoryInternal(newUrl);
    })
  }

  var vM = new StoryDetailViewModel();
  window.StoryDetailViewModel = vM;
  ko.applyBindings(vM, $("#storyDetail")[0]);
});