import json

from flask import Flask, abort, make_response, request, render_template

from constants import *
from clusterManager import ClusterManager

app = Flask(__name__)

#start helper functions

def getJsonResponse(retValue):
  resp = make_response(json.dumps(retValue), 200)
  resp.headers['Content-Type'] = 'application/json;charset=utf-8'

  return resp

#end helper functions

#start validations

def validateLocale(value):
  try:
    return next(x for x in ALLOWED_LOCALES if x.lower() == value.lower())
  except StopIteration:
    abort(400, "Allowed locale values are: " + str(ALLOWED_LOCALES))

def validateCategory(value):
  try:
    return next(x for x in ALLOWED_CATEGORIES if x.lower() == value.lower())
  except StopIteration:
    abort(400, "Allowed category values are: " + str(ALLOWED_CATEGORIES))

def validateCountry(value):
  try:
    return next(x for x in ALLOWED_COUNTRIES if x.lower() == value.lower())
  except StopIteration:
    abort(400, "Allowed country values are: " + str(ALLOWED_COUNTRIES))

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

#end validations

@app.route('/api/stories', methods=['GET'])
def get_stories():
  countryFilter = request.args.get('country')
  categoryFilter = request.args.get('category')
  localeFilter = request.args.get('locale')
  (skip, top) = validateSkipAndTop(
    request.args.get('skip'),
    request.args.get('top'))

  clusterManager = ClusterManager()
  if localeFilter and not (countryFilter or categoryFilter):
    localeFilter = validateLocale(localeFilter)
    return getJsonResponse(clusterManager.queryByLocale(
      localeFilter,
      skip,
      top))
  elif (countryFilter and categoryFilter) and not localeFilter:
    countryFilter = validateCountry(countryFilter)
    categoryFilter = validateCategory(categoryFilter)
    return getJsonResponse(clusterManager.queryByCategoryAndCountry(
      categoryFilter,
      countryFilter,
      skip,
      top))

  abort(400, "Invalid query")

@app.route('/all')
def show_all():
  return render_template(
    'all.html',
    sections = [
      {'id': 'localNews', 'title': 'Local'},
      {'id': 'nationalNews', 'title': 'National'},
      {'id': 'businessNews', 'title': 'Business'},
      {'id': 'worldNews', 'title': 'World'},
      {'id': 'sportsNews', 'title': 'Sports'}])

@app.route('/')
def home():
  return render_template(
    'home.html')

if __name__ == '__main__':
  app.run()
