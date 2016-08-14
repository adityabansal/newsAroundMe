$(function() {
  function onNavigateWithoutTracking() {
    newPath = location.pathname;
    window.SectionDropdownViewModel.loadSection(newPath.substr(1));
  }

  function onNavigate() {
    onNavigateWithoutTracking();

    ga('set', 'page', location.pathname);
    ga('send', 'pageview');
  }

  // Navigate whenever the fragment identifier value changes.
  window.addEventListener("popstate", onNavigate)

  // helper function to trigger navigation
  window.navigateTo = function(section) {
  	if (!!history.pushState) {
      history.pushState(
        section,
        section.title,
        "/" + section.value);
      onNavigate();
    } else {
      // this is to support old browsers
      window.open("/" + section.value, "_self")
    }
  }

  // Navigate once to the initial hash value.
  onNavigateWithoutTracking();
});