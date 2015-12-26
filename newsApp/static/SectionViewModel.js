$(function() {
  function StoryViewModel(story) {
    var self = this
    self.mainArticle = story[0]

    self.relatedArticles = null
    if (story.length > 1) {
      self.relatedArticles = ko.observableArray(story.slice(1, 6))
    }
  }

  function SectionViewModel(url) {
    var self = this;
    self.stories = ko.observableArray();
    self.panelText = ko.observable("Loading ...");

    $.getJSON(url, function( stories ) {
      var items = [];
      $.each( stories, function( index, story ) {
        self.stories.push(new StoryViewModel(story))
      });
      self.panelText("")
    });
  }

  if ($("#localNews").length === 1) {
      var localNewsVM = new SectionViewModel("/api/stories?locale=bangalore");
      ko.applyBindings(localNewsVM, $("#localNews")[0]);
  }

  if ($("#nationalNews").length === 1) {
      var localNewsVM = new SectionViewModel("/api/stories?category=national&country=india");
      ko.applyBindings(localNewsVM, $("#nationalNews")[0]);
  }

  if ($("#businessNews").length === 1) {
      var localNewsVM = new SectionViewModel("/api/stories?category=business&country=india");
      ko.applyBindings(localNewsVM, $("#businessNews")[0]);
  }

  if ($("#sportsNews").length === 1) {
      var localNewsVM = new SectionViewModel("/api/stories?category=sports&country=india");
      ko.applyBindings(localNewsVM, $("#sportsNews")[0]);
  }

  if ($("#worldNews").length === 1) {
      var localNewsVM = new SectionViewModel("/api/stories?category=world&country=india");
      ko.applyBindings(localNewsVM, $("#worldNews")[0]);
  }
});
