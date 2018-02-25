$(function() {
  function onNavigateWithoutTracking() {
    newPath = location.pathname.toLowerCase();

    if (newPath.indexOf("/story/") === 0) {
      window.StoryDetailViewModel.loadStory(newPath.substr(7))

      $("#section-filters").hide();
      $("#storyList").hide();
      $("#storyDetail").show();
    } else {
      window.SectionDropdownViewModel.loadSection(newPath.substr(1));

      $("#section-filters").show();
      $("#storyList").show();
      $("#storyDetail").hide();
    }
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

      // update page title
      document.title = section.title;
       // update meta description. Just replacing the value of the 'content' attribute will not work.
      $('meta[name=description]').remove();
      $('head').append("<meta name=\"description\" content=\"" + section.description + "\">");

      onNavigate();
    } else {
      // this is to support old browsers
      window.open("/" + section.value, "_self")
    }
  }

  // Navigate once to the initial hash value.
  onNavigateWithoutTracking();
});