var appCacheName = 'shellCache-v17';
var filesToCache = [
  '/static/ie10-viewport-bug-workaround.css',
  '/static/ie10-viewport-bug-workaround.js',
  '/static/bootstrap-3.3.6/css/bootstrap.min.css',
  '/static/bootstrap-3.3.6/js/bootstrap.min.js',
  '/static/bootstrap-3.3.6/fonts/glyphicons-halflings-regular.woff2',
  '/static/loading.svg',
  '/static/favicon.ico',
  '/static/logoNavbar.png'
];
// We have only cached those files which aren't expected to change.
// As we can't cache html due to dynamic urls, it's safer to leave javascript and css code out.

var storiesCacheName = 'dataStories-v2';

self.addEventListener('install', function(e) {
  console.log('[ServiceWorker] Install');
  e.waitUntil(
    caches.open(appCacheName).then(function(cache) {
      console.log('[ServiceWorker] Caching app shell');
      return cache.addAll(filesToCache);
    })
  );
});

self.addEventListener('activate', function(e) {
  console.log('[ServiceWorker] Activate');
  e.waitUntil(
    caches.keys().then(function(keyList) {
      return Promise.all(keyList.map(function(key) {
        if (key !== appCacheName && key !== storiesCacheName) {
          console.log('[ServiceWorker] Removing old cache', key);
          return caches.delete(key);
        }
      }));
    })
  );
  return self.clients.claim();
});

self.addEventListener('fetch', function(e) {
  console.log('[ServiceWorker] Fetch', e.request.url);

  var relativeUrl = e.request.url.replace(/^(?:\/\/|[^\/]+)*\//, "");

  if (relativeUrl.indexOf("api/stories") === 0 &&
    relativeUrl.indexOf("skip") === -1 &&
    relativeUrl.indexOf("top") === -1) {
    console.log('[ServiceWorker] Adding to storiesCache: ', e.request.url)

    // cache then network
    e.respondWith(
      caches.open(storiesCacheName).then(function(cache) {
        return fetch(e.request).then(function(response){
          cache.put(e.request.url, response.clone());
          return response;
        });
      })
    );
  } else {
    e.respondWith(
      caches.match(e.request).then(function(response) {
        return response || fetch(e.request);
      })
    );
  }
});