$(function() {
  function guidGenerator() {
    var S4 = function() {
      return (((1+Math.random())*0x10000)|0).toString(16).substring(1);
    };
    return (S4()+S4()+"-"+S4()+"-"+S4()+"-"+S4()+"-"+S4()+S4()+S4());
  }

  function cropText(text, maxLength) {
    return text.length > maxLength ? text.substring(0, maxLength) + "..." : text;
  }

  function StoryViewModel(story) {
    var self = this
    self.mainArticle = story[0]
    self.mainArticle.summaryText = cropText(self.mainArticle.summaryText, 500)

    self.relatedArticles = null
    if (story.length > 1) {
      self.relatedArticles = ko.observableArray(story.slice(1, 6))
    }

    self.carouselId = guidGenerator();
    self.images = ko.observableArray()
    $.each(story, function(index, article) {
      if ($.isArray(article.images)) {
        $.each(article.images, function(index, image) {
          var added = false;

          $.map(self.images(), function(elementOfArray, indexInArray) {
            if (elementOfArray.src == image) {
              added = true;
            }
          })

          if (!added) {
            self.images.push({
              'src': image,
              'link': article.link
            })
          }
        })
      }
    });
  }

  function SectionViewModel(url) {
    var self = this;
    self.stories = ko.observableArray();
    self.panelText = ko.observable("");

    self.loadData = function(url) {
      self.panelText("Loading ...");
      self.url = ko.observable(url);
      self.stories([])

      $.getJSON(url, function( stories ) {
        $.each( stories, function( index, story ) {
          self.stories.push(new StoryViewModel(story))
        });
        self.panelText("");
      });
    }

    self.loadMoreStories = function() {
      var urlWithSkipAndTop = self.url() + "&skip=" + self.stories().length + "&top=5";
      self.panelText("Loading ...");
      $.getJSON(urlWithSkipAndTop, function(stories) {
        $.each( stories, function( index, story ) {
          self.stories.push(new StoryViewModel(story))
        });
        self.panelText("");
      });
    }

    if (!!url) {
      self.loadData(url);
    }

    self.availableLocations = ko.observableArray(['bangalore', 'delhi'])
    self.location = ko.observable('')
    self.location.subscribe(function(newValue) {
      self.loadData("/api/stories?locale=" + newValue)
    })
  }

  if ($("#localNews").length === 1) {
      var localNewsVM = new SectionViewModel();
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
