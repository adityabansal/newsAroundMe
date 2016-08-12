$(function() {
  function onNavigateWithoutTracking() {
  	newHash = location.hash;
  	if (!!newHash) {
  	  window.SectionDropdownViewModel.loadSection(newHash.substr(1))
  	} else {
      window.SectionDropdownViewModel.loadSection("");
  	}
  }

  function onNavigate() {
    onNavigateWithoutTracking();

  	ga('set', 'page', newHash);
    ga('send', 'pageview');
  }

  // Navigate whenever the fragment identifier value changes.
  window.addEventListener("hashchange", onNavigate)

  // helper function to trigger navigation
  window.navigateTo = function(nash) {
  	location.hash = nash;
  }

  // Navigate once to the initial hash value.
  onNavigateWithoutTracking()
});