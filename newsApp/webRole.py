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

#end validations

@app.route('/api/stories', methods=['GET'])
def get_stories():
  countryFilter = request.args.get('country')
  categoryFilter = request.args.get('category')
  localeFilter = request.args.get('locale')

  clusterManager = ClusterManager()
  if localeFilter and not (countryFilter or categoryFilter):
    localeFilter = validateLocale(localeFilter)
    return getJsonResponse(clusterManager.queryByLocale(localeFilter))
  elif (countryFilter and categoryFilter) and not localeFilter:
    countryFilter = validateCountry(countryFilter)
    categoryFilter = validateCategory(categoryFilter)
    return getJsonResponse(clusterManager.queryByCategoryAndCountry(
      categoryFilter,
      countryFilter))

  abort(400, "Invalid query")

@app.route('/')
def show_entries():
  return render_template(
    'home.html',
    sections = [
      {'id': 'localNews', 'title': 'bangalore'},
      {'id': 'businessNews', 'title': 'Business'},
      {'id': 'sportsNews', 'title': 'Sports'},
      {'id': 'worldNews', 'title': 'World'},
      {'id': 'nationalNews', 'title': 'National'}])

if __name__ == '__main__':
  app.run()
