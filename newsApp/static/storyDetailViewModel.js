$(function() {
  function StoryDetailViewModel() {
    var self = this;
    self.docId = null;
    self.articles = ko.observableArray([]);
    self.locations = ko.observableArray([]);
    self.languageFilterVM = new window.LanguageFilterViewModel();
    self.lastRequest = null;
    self.isDataLoading = ko.observable(false);

    self.loadStoryInternal = function(url) {

      if(!!self.lastRequest && self.isDataLoading()) {
        self.lastRequest.abort();
      }

      self.isDataLoading(true);

      // reinitialize everything and fill with new values once the call is complete
      self.articles([]);
      self.locations([]);
      self.languageFilterVM.languages([]);
      self.lastRequest = $.getJSON(url, function( response ) {
        self.articles([]);
        self.articles(response.articles.map(article =>
          new window.ArticleViewModel(article)));

        self.locations(response.locales.map(function(locale) {
          return window.locationsMetadata.filter(location =>
            location.value === locale)[0];
        }));

        self.languageFilterVM.setLanguages(response.languages.map(function(langCode) {
          return window.languagesMetadata.filter(language =>
            language.id === langCode)
        }));
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

    self.navigateToLocale = function(location) {
      window.navigateTo(location);
    }
  }

  var vM = new StoryDetailViewModel();
  window.StoryDetailViewModel = vM;
  ko.applyBindings(vM, $("#storyDetail")[0]);
});