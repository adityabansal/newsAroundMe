$(function() {
  function SectionDropdownViewModel() {
    var self = this;
    self.label = ko.observable("Loading");
    self.disabled = ko.observable(true);
    self.selection = ko.observable();

    self.locations = ko.observableArray(
    	[{displayName: "Delhi", icon: "/static/delhi.svg", value: 'delhi'},
    	 {displayName: "Bangalore", icon: "/static/city.svg", value: 'bangalore'}])

    self.otherSections = ko.observableArray(
    	[{displayName: "National", icon: "/static/national.svg", value: 'national'},
    	 {displayName: "World", icon: "/static/world.svg", value: 'world'},
    	 {displayName: "Business", icon: "/static/business.svg", value: 'business'},
    	 {displayName: "Sports", icon: "/static/sports.svg", value: 'sports'}])

    self.selectSection = function(selectedSection) {
    	var locationMatches, otherMatches;

    	locationMatches = $.grep(self.locations(), function(location, index) {
      	    return location.value === selectedSection;
        })

        if (locationMatches.length > 0) {
        	self.selection(selectedSection);
        	self.label(locationMatches[0].displayName);
        	window.StoriesViewModel.loadData("/api/stories?locale=" + selectedSection);
        	return;
        }

        otherMatches = $.grep(self.otherSections(), function(section, index) {
      	    return section.value === selectedSection;
        })

        if (otherMatches.length > 0) {
        	self.selection(selectedSection);
        	self.label(otherMatches[0].displayName);
        	window.StoriesViewModel.loadData("/api/stories?category=" + selectedSection + "&country=india");
        	return;
        }
    }

    self.chooseSection = function(section) {
    	self.selectSection(section.value)
    }

    self.loadStoriesForUserLocation = function() {
    	self.disabled = ko.observable(true);
        $.getJSON('http://ipinfo.io', function(data){
            var city = data.city,
                selectedLocation;

            // handle synonyms
            if (city.toLowerCase() === 'bengaluru') {
      	        city = 'bangalore'
            }

            match = $.grep(self.locations(), function(location, index) {
      	        return location.value.toLowerCase() === city.toLowerCase();
            })

            if (match.length > 0) {
      	        selectedLocation = match[0]
            } else {
      	        selectedLocation = self.locations()[0];
            }

            self.selectSection(selectedLocation.value);
            self.disabled(false)
        });
    }

    self.loadStoriesForUserLocation();
  }

  // bind the viewModel
  var vM = new SectionDropdownViewModel();
  window.SectionDropdownViewModel = vM;
  ko.applyBindings(vM, $("#section-dropdown")[0]);

});