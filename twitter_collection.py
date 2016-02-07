# -*- coding: utf-8 -*-

from time import sleep
from twython import Twython, \
                    TwythonError, \
                    TwythonRateLimitError, \
                    TwythonAuthError


RATE_LIMIT_WINDOW = 15 * 60
WAIT_BETWEEN_AUTH = 10 * 60
TWEET_BATCH_SIZE = 200
FOLLOWER_BATCH_SIZE = 200

class TwitterCollection:

    """Class for basic Twitter data collection"""

    def __init__(self, APP_KEY, APP_SECRET):
        self.APP_KEY = APP_KEY
        self.APP_SECRET = APP_SECRET

    def authenticate(self):
        """
        Authenticate to Twitter
        """
        while True:
            print('Authenticating to Twitter...\n')
            try:
                twitter = Twython(APP_KEY, APP_SECRET, oauth_version=2)
                ACCESS_TOKEN = twitter.obtain_access_token()
                ret = Twython(APP_KEY, access_token=ACCESS_TOKEN)
                print('Authentication successful\n')
                return ret
            except TwythonAuthError, e:
                traceback.print_exc()
                sleep(WAIT_BETWEEN_AUTH)

    def get_followers(self, screen_name, num_followers):
        """
        Get followers for a given Twitter screen name batch by batch
        """
        twitter = authenticate(APP_KEY, APP_SECRET)

        users_downloaded = 0
        next_cursor = -1

        while users_downloaded < num_followers & num_followers is not None:
            try:
                response = twitter.get_followers_list(
                    screen_name=screen_name,
                    count=FOLLOWER_BATCH_SIZE,
                    cursor=next_cursor)
            except TwythonRateLimitError:
                print('Rate limit reached. Sleeping...\n')
                sleep(RATE_LIMIT_WINDOW)
                continue
            except TwythonError:
                print('Twython error encountered')
                continue
            except KeyError:
                continue

            for follower in response['users']:
                print(follower['screen_name'])

            next_cursor = response['next_cursor']
            users_downloaded += FOLLOWER_BATCH_SIZE

    def get_statuses(self, screen_name, num_tweets):
        """
        Get statuses for a given Twitter screen name batch by batch
        """
        twitter = authenticate(APP_KEY, APP_SECRET)

        tweets_collected = 0
        max_tweet_id = 0

        while tweets_collected < num_tweets and num_tweets is not None:
            tweets = []
            try:
                if max_tweet_id == 0:
                    tweets = twitter.get_user_timeline(
                        screen_name=screen_name, 
                        count=TWEET_BATCH_SIZE)
                else:
                    tweets = twitter.get_user_timeline(
                        screen_name=screen_name,
                        max_id=max_tweet_id,
                        count=TWEET_BATCH_SIZE)
                tweets_collected += TWEET_BATCH_SIZE
            except TwythonRateLimitError:
                print('Rate limit reached. Sleeping...\n')
                sleep(RATE_LIMIT_WINDOW)
                continue
            except TwythonError:
                print('Twython error encountered')
                break

            for tweet in tweets:
                if max_tweet_id == 0 or max_tweet_id > int(tweet['id']):
                    max_tweet_id = int(tweet['id'])
                print tweet['text']
