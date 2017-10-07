var appCacheName = 'shellCache-v1';
var filesToCache = [
  '/',
  '/static/ie10-viewport-bug-workaround.css',
  '/static/custom.css',
  '/static/ie10-viewport-bug-workaround.js',
  '/static/gen/home_packed.js',
  '/static/logoNavbar.png',
  '/static/loading.gif',
  '/static/favicon.ico'
];

var storiesCacheName = 'dataStories-v1';

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
  console.log('[ServiceWorker] Relative url is: ', relativeUrl)

  if (relativeUrl.indexOf("api/stories") === 0) {
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