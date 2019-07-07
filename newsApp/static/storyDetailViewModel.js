$(function() {
  function StoryDetailViewModel() {
    var self = this;
    self.docId = null;
    self.articles = ko.observableArray([]);
    self.locations = ko.observableArray([]);
    self.languageFilterVM = new window.LanguageFilterViewModel();
    self.lastRequest = null;
    self.isDataLoading = ko.observable(false);

    self.mainCity = ko.observable();
    self.cityStories = ko.observableArray([]);
    self.loadCityStories = function(stories) {
      self.cityStories([]);
      stories = stories.slice(0, 4);
      $.each(stories, function( index, story ) {
        self.cityStories.push(new window.StoryViewModel(story));
      });
    }
    self.cityStoriesDataLoader = new window.DataLoaderModel();

    self.loadStoryInternal = function(url, refreshLanguageFilter) {

      if(!!self.lastRequest && self.isDataLoading()) {
        self.lastRequest.abort();
      }

      self.isDataLoading(true);

      if (refreshLanguageFilter) {
        self.locations([]);
        self.languageFilterVM.setLanguages([]);
      }

      self.articles([]);
      self.cityStories([]);

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
        self.mainCity(self.locations()[0])

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

    self.mainCity.subscribe(function(newLocation) {
      self.cityStoriesDataLoader.initialize(
        "/api/stories?locale=" + newLocation.value,
        self.loadCityStories)
      self.cityStoriesDataLoader.loadDataOfflineFirst()
    })
  }

  var vM = new StoryDetailViewModel();
  window.StoryDetailViewModel = vM;
  ko.applyBindings(vM, $("#storyDetail")[0]);
});