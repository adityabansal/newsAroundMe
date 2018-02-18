$(function() {
  function cropText(text, maxLength) {
    if (!text) {
      return ""
    }
    return text.length > maxLength ? text.substring(0, maxLength) + "..." : text;
  }

  function StoryViewModel(story) {
    var self = this,
        articleVMs = story.articles.map(article => new window.ArticleViewModel(article));
    self.mainArticle = articleVMs[0];

    self.relatedArticles = null;
    if (articleVMs.length > 1) {
      self.relatedArticles = ko.observableArray(articleVMs.slice(1, 6));
    }

    self.images = ko.observableArray();
    $.each(articleVMs, function(index, article) {
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
              'link': article.link,
              'publisher': article.publisher
            })
          }
        })
      }
    });

    self.navigateToDetails = function() {
      window.navigateTo({
        title: (self.mainArticle.title + " - Full coverage by newsAroundMe"),
        description: "See this and related articles at newsaroundme.com",
        value: "story/" + self.mainArticle.id
      });
    }
  }

  window.StoryViewModel = StoryViewModel;
});