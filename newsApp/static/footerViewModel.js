$(function() {
  function FooterViewModel() {
    var self = this;

    self.locations = ko.observableArray(window.locationsMetadata);

    self.navigateToLocation = function(location) {
      window.navigateTo(location);
    }
  }

  // bind the viewModel
  var vM = new FooterViewModel();
  window.FooterViewModel = vM;
  ko.applyBindings(vM, $(".footer-container")[0]);

});