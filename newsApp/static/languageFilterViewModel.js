$(function() {
  function LanguageFilterViewModel() {
    var self = this;
    self.languages = ko.observableArray([]);
    // The label to be shown in language dropdown
    self.langLabel = ko.computed(function() {
      var languageNames = [], allLanguageNames = [];

      self.languages().forEach(function(language) {
        if (language.selected()) {
          languageNames.push(language.displayName);
        }
        allLanguageNames.push(language.displayName);
      })

      // in case user unselects all languages, select all of them back
      if (languageNames.length === 0) {
        self.languages().forEach(function(language) {
          language.selected(true);
        });

        return allLanguageNames.join(",");
      } else {
        return languageNames.join(",");
      }
    });

    self.languageFilter = ko.computed(function() {
      var languageCodes = [];

      self.languages().forEach(function(language) {
        if (language.selected()) {
          languageCodes.push(language.id);
        }
      })

      if ((languageCodes.length === 0) ||
          (languageCodes.length === self.languages().length)) {

        return ""; // no language filter needed
      } else {
        return "languages=" + languageCodes.join(",");
      }
    })

    self.setLanguages = function(languageArray) {
      newLanguageArray =languageArray.map(function(language) {
        return $.extend({}, language, {
          "selected" : ko.observable(true)
        });
      });

      self.languages([]);
      self.languages(newLanguageArray)
    }
  }

  window.LanguageFilterViewModel = LanguageFilterViewModel;
});