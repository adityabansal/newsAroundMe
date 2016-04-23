$(function() {
  function guidGenerator() {
    var S4 = function() {
      return (((1+Math.random())*0x10000)|0).toString(16).substring(1);
    };
    return (S4()+S4()+"-"+S4()+"-"+S4()+"-"+S4()+"-"+S4()+S4()+S4());
  }

  function cropText(text, maxLength) {
    if (!text) {
      return ""
    }
    return text.length > maxLength ? text.substring(0, maxLength) + "..." : text;
  }

  function StoryViewModel(story) {
    var self = this;
    self.mainArticle = story[0];
    self.mainArticle.summaryTextToDisplay = cropText(self.mainArticle.summaryText, 100)

    self.relatedArticles = null;
    if (story.length > 1) {
      self.relatedArticles = ko.observableArray(story.slice(1, 6));
    }

    self.carouselId = guidGenerator();
    self.images = ko.observableArray();
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
              'link': article.link,
              'publisher': article.publisher
            })
          }
        })
      }
    });
  }

  window.StoryViewModel = StoryViewModel;
});