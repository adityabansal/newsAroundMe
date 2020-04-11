### See PIN-based authorization for details at
### https://dev.twitter.com/docs/auth/pin-based-authorization
 
import tweepy
from .notifierTwitter import NotifierTwitter
 
consumer_key= input('consumerKey: ').strip()
consumer_secret= input('consumerSecret: ').strip()

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

# get access token from the user and redirect to auth URL
auth_url = auth.get_authorization_url()
print('Authorization URL: ' + auth_url)

# ask user to verify the PIN generated in broswer
verifier = input('PIN: ').strip()
auth.get_access_token(verifier)
token = auth.access_token
tokenSecret = auth.access_token_secret


auth.set_access_token(token, tokenSecret)
api = tweepy.API(auth)
username = api.me().name
print('Username: ' + username)

nt = NotifierTwitter()
handle = input('handle: ').strip()
nt.addHandle(handle, consumer_key, consumer_secret, token, tokenSecret)