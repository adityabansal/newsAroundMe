$(function() {
  function AppUtils() {
    var self = this;
    self.cropText = function (text, maxLength) {
      var trimmedText = text.trim();

      if (!trimmedText) {
        return ""
      }
      return trimmedText.length > maxLength ?
        trimmedText.substring(0, maxLength) + "..." : trimmedText;
    }

    self.formatDate = function (date){
      var monthNames = [
        "January", "February", "March",
        "April", "May", "June", "July",
        "August", "September", "October",
        "November", "December"
      ];

      var day = date.getDate();
      var monthIndex = date.getMonth();
      var year = date.getFullYear();

      return day + ' ' + monthNames[monthIndex] + ' ' + year;
    }

    self.computeTimeString = function (time) {
      var inputDate,
        currentTime = Math.round(new Date()/1000),
        diff = currentTime - time;

      if (diff < 60*60) {
        return "Less than 1 hour ago"
      } else if (diff < 2*60*60) {
        return "1 hour ago"
      } else if (diff < 24*60*60) {
        return Math.floor(diff/(60*60)) + " hours ago"
      } else {
        inputDate = new Date(time * 1000);
        return self.formatDate(inputDate);
      }
    }
  }

  window.AppUtils = new AppUtils();
});