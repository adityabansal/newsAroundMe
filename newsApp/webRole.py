import json

from flask import Flask, abort, redirect, url_for
from flask import make_response, request, render_template
from flask_assets import Environment, Bundle

from constants import *
from clusterManager import ClusterManager
from imageProcessor import ImageProcessor

app = Flask(__name__)

app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

assets = Environment(app)
home_js = Bundle(
  'init.js',
  'appUtils.js',
  'languageFilterViewModel.js',
  'sectionDropdownViewModel.js',
  'articleViewModel.js',
  'storyViewModel.js',
  'storyDetailViewModel.js',
  'storiesViewModel.js',
  'footerViewModel.js',
  'navigate.js',
  'app.js',
  filters='jsmin',
  output='gen/home_packed.js')
assets.register('home_js', home_js)

#start helper functions

def getJsonResponse(retValue):
  resp = make_response(json.dumps(retValue), 200)
  resp.headers['Content-Type'] = 'application/json;charset=utf-8'

  return resp

def getSharingMetaTags(title, description, imageUrl, ogType):
  if not ogType:
    ogType = 'website'

  metaTags = [
    { 'tagType': u'name="twitter:card"', 'content': 'summary' },
    { 'tagType': u'property="og:url"', 'content': request.base_url },
    { 'tagType': u'property="og:type"', 'content': ogType},
    { 'tagType': u'property="og:title"', 'content': title },
    { 'tagType': u'property="og:description"', 'content': description }];

  if imageUrl:
    imageUrl = request.host_url + imageUrl;
    metaTags.append({ 'tagType': u'property="og:image"', 'content': imageUrl })

  return metaTags;

#end helper functions

#start validations

def validateLocale(value):
  try:
    return next(x['value'] for x in LOCATION_METADATA if \
      x['value'].lower() == value.lower())
  except StopIteration:
    abort(400, "Invalid location specified")

def validateCategory(value):
  try:
    return next(x for x in ALLOWED_CATEGORIES if x.lower() == value.lower())
  except StopIteration:
    abort(400, "Allowed category values are: " + json.dumps(ALLOWED_CATEGORIES))

def validateCountry(value):
  try:
    return next(x for x in ALLOWED_COUNTRIES if x.lower() == value.lower())
  except StopIteration:
    abort(400, "Allowed country values are: " + json.dumps(ALLOWED_COUNTRIES))

def validateSkipAndTop(skip, top):
  # if not specified return default values"
  if not skip and not top:
    return (0, 5)

  if not skip or not top:
    abort(400, "Specify both skip and top")

  try:
    parsedSkip = int(skip)
    parsedTop = int(top)
  except ValueError:
    abort(400, "skip and top values should be integers")

  if parsedSkip < 0 or parsedTop < 0:
    abort(400, "skip and top cannot not be negative")

  if parsedTop > 5:
    abort(400, "Top cannot be greater than 5")

  return (parsedSkip, parsedTop)

def validateFilters(requestArgs):
  parsedFilters = {}

  if CLUSTERS_FILTER_LANGUAGES in requestArgs:
    parsedLangs = requestArgs.get(CLUSTERS_FILTER_LANGUAGES).split(",");

    for lang in parsedLangs:
      try:
        next(x for x in AVAILABLE_LANGUAGES if x['id'].lower() == lang.lower())
      except StopIteration:
        abort(400, "Unsupported language code: " + lang)

    parsedFilters[CLUSTERS_FILTER_LANGUAGES] = parsedLangs

  return parsedFilters

#end validations

@app.before_request
def enforceHttpsInHeroku():
  if request.headers.get('X-Forwarded-Proto') == 'http':
    url = request.url.replace('http://', 'https://', 1)
    code = 301
    return redirect(url, code=code)

@app.route('/service-worker.js')
def static_file():
    return app.send_static_file('service-worker.js')

@app.route('/api/stories', methods=['GET'])
def get_stories():
  countryFilter = request.args.get('country')
  categoryFilter = request.args.get('category')
  localeFilter = request.args.get('locale')
  (skip, top) = validateSkipAndTop(
    request.args.get('skip'),
    request.args.get('top'))
  filters = validateFilters(request.args)

  clusterManager = ClusterManager()
  if localeFilter and not (countryFilter or categoryFilter):
    localeFilter = validateLocale(localeFilter)
    return getJsonResponse(clusterManager.queryByLocale(
      localeFilter,
      skip,
      top,
      filters))
  elif (countryFilter and categoryFilter) and not localeFilter:
    countryFilter = validateCountry(countryFilter)
    categoryFilter = validateCategory(categoryFilter)
    return getJsonResponse(clusterManager.queryByCategoryAndCountry(
      categoryFilter,
      countryFilter,
      skip,
      top,
      filters))

  abort(400, "Invalid query")

@app.route('/api/story/<docId>', methods=['GET'])
def get_story(docId):
  filters = validateFilters(request.args)

  clusterManager = ClusterManager()
  cluster = clusterManager.queryByDocId(docId.upper(), filters)
  if not cluster:
    abort(404)
  else:
    return getJsonResponse(cluster)

@app.route('/images/<imageKey>')
def getImage(imageKey):
  imageProcessor = ImageProcessor()
  imageContent = imageProcessor.getImageContent(imageKey)

  if not imageContent:
    return abort(404);
  else:
    response = make_response(imageContent)
    response.headers['Content-Type'] = 'image/jpeg'
    return response

@app.route('/')
def home():
  return render_template(
    'home.html',
    title='newsAroundMe',
    description='Latest local news from your location',
    locationsMetadata=json.dumps(LOCATION_METADATA),
    languagesMetadata=json.dumps(AVAILABLE_LANGUAGES))

@app.route('/<location>')
def loadLocationPage(location):
  matchingLocation = [x for x in LOCATION_METADATA if \
    x['value'].lower() == location.lower()]

  if not matchingLocation:
    return redirect(url_for('home'))
  return render_template(
    'home.html',
    title=matchingLocation[0]['title'],
    description=matchingLocation[0]['description'],
    locationsMetadata=json.dumps(LOCATION_METADATA),
    languagesMetadata=json.dumps(AVAILABLE_LANGUAGES),
    metaTags=getSharingMetaTags(
      matchingLocation[0]['title'],
      matchingLocation[0]['description'],
      None,
      None))

@app.route('/story/<docId>')
def loadStoryPage(docId):
  clusterManager = ClusterManager()
  cluster = clusterManager.queryByDocId(docId.upper())

  if not cluster:
    return redirect(url_for('home'))

  imagesByArticle = [article['images'] for article in cluster['articles']]
  allImages = [image for articleImages in imagesByArticle for image in articleImages]
  image = allImages[0] if allImages else None

  return render_template(
    'home.html',
    title=cluster['title'],
    description=cluster['description'],
    locationsMetadata=json.dumps(LOCATION_METADATA),
    languagesMetadata=json.dumps(AVAILABLE_LANGUAGES),
    metaTags=getSharingMetaTags(
      cluster['title'],
      cluster['description'],
      image,
      'article'))

if __name__ == '__main__':
  app.run()
