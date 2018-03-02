$(function() {
  function StoryDetailViewModel() {
    var self = this;
    self.docId = null;
    self.articles = ko.observableArray([]);
    self.locations = ko.observableArray([]);
    self.languageFilterVM = new window.LanguageFilterViewModel();
    self.lastRequest = null;
    self.isDataLoading = ko.observable(false);

    self.loadStoryInternal = function(url, refreshLanguageFilter) {

      if(!!self.lastRequest && self.isDataLoading()) {
        self.lastRequest.abort();
      }

      self.isDataLoading(true);

      self.articles([]);
      self.lastRequest = $.getJSON(url, function( response ) {
        self.articles([]);
        self.articles(response.articles.map(function(article) {
          return new window.ArticleViewModel(article);
        }));

        self.locations(response.locales.map(function(locale) {
          return window.locationsMetadata.filter(function(location) {
            return location.value === locale;
          })[0];
        }));

        if (!!refreshLanguageFilter) {
          self.languageFilterVM.setLanguages(response.languages.map(function(langCode) {
            return window.languagesMetadata.filter(function(language) {
              return (language.id === langCode);
            })[0];
          }));
        }
      });

      self.lastRequest.always(function() {
        self.isDataLoading(false);
      });
    }

    self.loadStory = function(docId) {
      self.docId = docId;
      var url = "/api/story/" + self.docId;

      self.loadStoryInternal(url, true)
    }

    self.languageFilterVM.languageFilter.subscribe(function(newValue) {
      var newUrl = "/api/story/" + self.docId;
      if (!!newValue) {
        newUrl = newUrl + "?" + newValue;
      }

      self.loadStoryInternal(newUrl, false);
    })

    self.navigateToLocale = function(location) {
      window.navigateTo(location);
    }
  }

  var vM = new StoryDetailViewModel();
  window.StoryDetailViewModel = vM;
  ko.applyBindings(vM, $("#storyDetail")[0]);
});