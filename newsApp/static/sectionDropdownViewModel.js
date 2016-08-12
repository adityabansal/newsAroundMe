$(function() {
  function SectionDropdownViewModel() {
    var self = this;
    self.label = ko.observable("Loading");
    self.disabled = ko.observable(false);
    self.selection = ko.observable();

    self.locations = ko.observableArray(
        [{displayName: "Delhi", icon: "/static/delhi.svg", value: 'delhi'},
         {displayName: "Mumbai", icon: '', value: 'mumbai'},
         {displayName: "Bangalore", icon: "/static/city.svg", value: 'bangalore'},
         {displayName: "Chennai", icon: '', value: 'chennai'},
         {displayName: "Kolkata", icon: '', value: 'kolkata'},
         {displayName: "Hyderabad", icon: '', value: 'hyderabad'},
         {displayName: "Pune", icon: '', value: 'pune'}])

    self.loadSection = function(section) {
      var locationMatch, url;

      locationMatch = self._getMatchingLocation(section);

      if (!!locationMatch) {
        self.selection(locationMatch);
        self.label(locationMatch.displayName);

        // update page title
        document.title = locationMatch.displayName + " News - newsAroundMe";
         // update meta description. Just replacing the value of the 'content' attribute will not work.
        $('meta[name=description]').remove();
        $('head').append("<meta name=\"description\" content=\"Latest local news from "+ locationMatch.displayName + ".\">");

        url = "/api/stories?locale=" + locationMatch.value;
      }

      if (!!url) {
        window.StoriesViewModel.loadData(url);
      } else {
        self.loadStoriesForUserLocation();
      }

      return;
    }

    self.chooseSection = function(section) {
      window.navigateTo(section.value)
    }

    self.loadStoriesForUserLocation = function() {
      self.disabled(true);
      $.getJSON('http://ipinfo.io', function(data){
        var city = data.city || "",
          match,
          selectedLocation;

        match = self._getMatchingLocation(city);

        if (!!match) {
          selectedLocation = match;
        } else {
          selectedLocation = self.locations()[0];
        }

        self.chooseSection(selectedLocation);
        self.disabled(false)
      });
    }

    self._getMatchingLocation = function(section) {
      // handle synonyms
      if (section.toLowerCase() === 'bengaluru') {
        section = 'bangalore'
      }

       var match = $.grep(self.locations(), function(location, index) {
        return location.value.toLowerCase() === section.toLowerCase();
      })

      return match[0];
    }
  }

  // bind the viewModel
  var vM = new SectionDropdownViewModel();
  window.SectionDropdownViewModel = vM;
  ko.applyBindings(vM, $("#section-dropdown")[0]);

});