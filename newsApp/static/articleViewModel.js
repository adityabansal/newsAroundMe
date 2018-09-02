$(function() {


  function ArticleViewModel(article) {
    var self = this;
    $.extend(this, article)
    self.summaryTextToDisplay = window.AppUtils.cropText(article.summaryText, 100);

    // add / before image url to make it relative to site root
    self.images = self.images.map(function(image) {
      return "/" + image;
    });

    self.timeString = window.AppUtils.computeTimeString(article.publishedOn);

    self.highlights = !!self.highlights ? self.highlights.slice(0, 4) : [];
  }

  window.ArticleViewModel = ArticleViewModel;
});