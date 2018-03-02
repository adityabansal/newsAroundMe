$(function() {
  function StoryViewModel(story) {
    var self = this,
        articleVMs = story.articles.map(function(article) {
          return new window.ArticleViewModel(article);
        });
    self.mainArticle = articleVMs[0];
    self.title = story.title;
    self.description = story.description;

    self.relatedArticles = null;
    if (articleVMs.length > 1) {
      self.relatedArticles = ko.observableArray(articleVMs.slice(1, 6));
    }

    self.images = ko.observableArray([]);
    $.each(articleVMs, function(index, article) {
      if ($.isArray(article.images)) {
        $.each(article.images, function(index, image) {
          var added = false;

          $.each(self.images(), function(indexInArray, elementOfArray) {
            if (elementOfArray.src === image) {
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

    self.storyDetailUrl = "/story/" + self.mainArticle.id;

    self.navigateToDetails = function() {
      window.navigateTo({
        title: self.title,
        description: self.description,
        value: self.storyDetailUrl.substr(1) //remove the initial '/''
      });
    }
  }

  window.StoryViewModel = StoryViewModel;
});