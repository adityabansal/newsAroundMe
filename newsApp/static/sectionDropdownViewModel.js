$(function() {
  function SectionDropdownViewModel() {
    var self = this;
    self.label = ko.observable("Loading");
    self.disabled = ko.observable(false);
    self.selection = ko.observable();

    self.locations = ko.observableArray(JSON.parse($("#locationMetadata")[0].innerHTML))

    self.loadSection = function(section) {
      var locationMatch, url;

      locationMatch = self._getMatchingLocation(section);

      if (!!locationMatch) {
        if (!!self.selection() && (locationMatch.value === self.selection().value)) {
          // the section is already loaded.
          return;
        }

        self.selection(locationMatch);
        self.label(locationMatch.displayName);

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

    self.chooseSection = function(section) {
      window.navigateTo(section)
    }

    self.loadStoriesForUserLocation = function() {
      self.disabled(true);
      $.getJSON('http://ipinfo.io', function(data){
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

        self.chooseSection(selectedLocation);
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

        return closest;
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