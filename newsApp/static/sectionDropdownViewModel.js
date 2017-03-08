$(function() {
  function SectionDropdownViewModel() {
    var self = this;
    self.cityLabel = ko.observable("Loading");
    self.disabled = ko.observable(false);

    self.location = ko.observable();
    self.locations = ko.observableArray(JSON.parse($("#locationMetadata")[0].innerHTML));


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
    self.languageFilter.subscribe(function(newValue) {
      var newUrl = "/api/stories?locale=" + self.location().value;
      if (!!newValue) {
        newUrl = newUrl + "&" + newValue;
      }

      console.log("Loading data for url" + newUrl);
      window.StoriesViewModel.loadData(newUrl);
    })

    self.loadSection = function(section) {
      var locationMatch, url, section;

      locationMatch = self._getMatchingLocation(section);

      if (!!locationMatch) {
        if (!!self.location() && (locationMatch.value === self.location().value)) {
          // the section is already loaded.
          return;
        }

        self.location(locationMatch);
        self.cityLabel(locationMatch.displayName);
        self.languages([]);
        $.each(locationMatch.languages, function(index, language) {
          self.languages.push(
            $.extend({}, language, {
              "selected" : ko.observable(true)
            }));
        });

        // update page title
        document.title = locationMatch.title;
         // update meta description. Just replacing the value of the 'content' attribute will not work.
        $('meta[name=description]').remove();
        $('head').append("<meta name=\"description\" content=\"" + locationMatch.description + "\">");

        url = "/api/stories?locale=" + locationMatch.value;
      }

      if (!!url) {
        window.StoriesViewModel.loadData(url);
      } else {
        // no or immproper location specified,
        // try to automatically find user location
        self.loadStoriesForUserLocation();
      }

      return;
    }

    self.chooseLocation = function(location) {
      window.navigateTo(location);
    }

    self.loadStoriesForUserLocation = function() {
      self.disabled(true);
      $.getJSON('https://ipinfo.io', function(data){
        var city = data.city || "",
          lat,
          long,
          match,
          selectedLocation;

        try {
          lat = parseFloat(data.loc.split(",")[0]);
          long = parseFloat(data.loc.split(",")[1]);
        } catch(err) {
          lat = null;
          long = null;
        }

        match = self._getMatchingLocation(city, lat, long);

        if (!!match) {
          selectedLocation = match;
        } else {
          selectedLocation = self.locations()[0];
        }

        self.chooseLocation(selectedLocation);
        self.disabled(false)
      });
    }

    self._getMatchingLocation = function(section, lat, long) {
      var match = $.grep(self.locations(), function(location, index) {
        return location.value.toLowerCase() === section.toLowerCase();
      })

      if ((match.length === 0) && !!lat && !!long) {
        var closest = self.locations()[0];

        $.each(self.locations(), function(index, location) {
          var distance = Math.pow(location.lat - lat, 2) + Math.pow(location.long - long, 2);
          var closestDistance = Math.pow(closest.lat - lat, 2) + Math.pow(closest.long - long, 2);

          if (distance < closestDistance) {
            closest = location;
          }
        })

        var closestDistance = Math.pow(closest.lat - lat, 2) + Math.pow(closest.long - long, 2);
        if (closestDistance < 200) {
          return closest;
        } else {
          // no nearby city found
          return self.locations()[0];
        }
      } else {
        return match[0];
      }
    }
  }

  // bind the viewModel
  var vM = new SectionDropdownViewModel();
  window.SectionDropdownViewModel = vM;
  ko.applyBindings(vM, $("#section-dropdown")[0]);

});