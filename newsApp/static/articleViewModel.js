$(function() {
  function cropText(text, maxLength) {
    if (!text) {
      return ""
    }
    return text.length > maxLength ? text.substring(0, maxLength) + "..." : text;
  }

  function ArticleViewModel(article) {
    var self = this;
    $.extend(this, article)
    self.summaryTextToDisplay = cropText(article.summaryText, 100);

    // add / before image url to make it relative to site root
    $.each(self.images,function(index, image) {
      image = "/" + image;
    })
  }

  window.ArticleViewModel = ArticleViewModel;
});