$(function() {
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/service-worker.js', {scope: '/'})
      .then(function(registration) {
        console.log('Service worker registration succeeded:', registration);
      }).catch(function(error) {
        console.log('Service worker registration failed:', error);
      });
  } else {
    console.log('Service workers are not supported.');
  }

  try {
    // This is so that footer comes near bottom of page for all screen sizes
    $(".body-content").css('min-height', screen.height - 200)
  } catch (e) {
    console.log('Failed to initialize body heignt')
  }
});