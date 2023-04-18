import pandas as pd
import tweepy
import time
import datetime


## ==============================
## Step 1. Authentication
## ==============================
def getAuthentication(bearer_token):

    print(f'\nObteniendo autenticación...')

    # Your app's bearer token can be found under the Authentication Tokens section
    # of the Keys and Tokens tab of your app, under the Twitter Developer Portal

    # You can authenticate as your app with just your bearer token
    client = tweepy.Client(bearer_token=bearer_token,
                            wait_on_rate_limit=True)

    return client


## ==============================
## Step 2. Search Historical Tweets
## ==============================
def searchHistoricalTweets(client, query, start_time, end_time, max_results=500, limit=10):

    # The full-archive search endpoint returns the complete history of public Tweets matching a search query;
    # since the first Tweet was created March 26, 2006.

    # By default, a request will return the most recent Tweets first (sorted by recency)

    print(f'\nBuscando tweets: {query}')

    response_lst = []

    for response in tweepy.Paginator(client.search_all_tweets,
                                    query,
                                    start_time=start_time,
                                    end_time=end_time,
                                    expansions='geo.place_id,author_id',
                                    place_fields=['full_name', 'id', 'country', 'country_code', 'geo', 'name', 'place_type'],
                                    tweet_fields=['author_id', 'created_at', 'lang', 'possibly_sensitive', 'public_metrics'],
                                    user_fields=['id', 'username', 'description', 'verified', 'public_metrics'],
                                    max_results=max_results,
                                    limit=limit):
        
        # Response metadata
        print(f'{response.meta["result_count"]} tweets encontrados...')
        
        # Sleep 2 seconds
        time.sleep(2)

        # Append raw response - Best practice to handle errors, etc.
        response_lst.append(response)
    

    return response_lst


## ==============================
## Step 3. Procesar Tweets
## ==============================
def processTweets(response_lst, filename_save):

    print(f'\nProcesando respuesta...')

    result = []
    user_dict = {}
    place_dict = {}

    # Loop though each response object:
    for response in response_lst:

        # Take all of the users, and put them into a dictionary of dictionaries with the info to keep
        for user in response.includes['users']:
            user_dict[user.id] = {'user_id': user.id,
                                    'user_username': user.username,
                                    'user_verified': user.verified,
                                    'user_protected': user.protected,
                                    'user_description': user.description,
                                    'user_profile_image_url': user.profile_image_url,
                                    'user_location': user.location,
                                    'user_followers': user.public_metrics['followers_count'],
                                    'user_following': user.public_metrics['following_count'],
                                    'user_tweet_count': user.public_metrics['tweet_count']
                                }
    
        # Take all of the places, and put them into a dictionary of dictionaries with the info to keep
        if 'places' in response.includes.keys():
            for place in response.includes['places']:
                place_dict[place.id] = {'place_id': place.id,
                                    'place_name': place.name,
                                    'place_full_name': place.full_name,
                                    'place_country': place.country,
                                    'place_country_code': place.country_code,
                                    'place_type': place.place_type
                                }
    
        # Save the tweets info
        for tweet in response.data:

            # For each tweet, find the author's information
            author_info = user_dict[tweet.author_id]
            place_info = place_dict[tweet.geo['place_id']] if tweet.geo else {'place_id': None, 'place_name': None, 'place_full_name': None,
                                                                                 'place_country': None, 'place_country_code': None, 'place_type': None}

            # Put all of the information we want to keep in a single dictionary for each tweet
            info = {
                'tweet_id': tweet.id,
                'tweet_text': tweet.text,
                'tweet_created_at': tweet.created_at,
                'tweet_source': tweet.source,
                'tweet_lang': tweet.lang,
                'tweet_possibly_sensitive': tweet.possibly_sensitive,
                'tweet_retweet_count': tweet.public_metrics['retweet_count'],
                'tweet_reply_count': tweet.public_metrics['reply_count'],
                'tweet_like_count': tweet.public_metrics['like_count'],
                'tweet_quote_count': tweet.public_metrics['quote_count'],
                'tweet_impression_count': tweet.public_metrics['impression_count'],
                'user_id': tweet.author_id,
                'user_username': author_info['user_username'],
                'user_verified': author_info['user_verified'],
                'user_protected': author_info['user_protected'],
                'user_description': author_info['user_description'],
                'user_profile_image_url': author_info['user_profile_image_url'],
                'user_location': author_info['user_location'],
                'user_followers': author_info['user_followers'],
                'user_following': author_info['user_following'],
                'user_tweet_count': author_info['user_tweet_count'],
                'place_id': place_info['place_id'],
                'place_name': place_info['place_name'],
                'place_full_name': place_info['place_full_name'],
                'place_country': place_info['place_country'],
                'place_country_code': place_info['place_country_code'],
                'place_type': place_info['place_type']
            }
            result.append(info)
    
    # Change the list of dictionaries into a dataframe
    df = pd.DataFrame(result)

    # Tamaño del dataframe
    print(f'Total tweets {df.shape}')

    # Exportarlo a csv
    print(f'Guardando :)')
    date_time = datetime.datetime.now().strftime('%d%m%y_%H%M%S%f') # Concatena al final fecha y hora
    final_name = f'{filename_save}_{date_time}.csv' # Concatena extensión de archivo
    df.to_csv(final_name, index=False)





# ======================================================================
bearer_token = ''

query = """-RT ("I was diagnosed" OR "I've been diagnosed" OR "I have been diagnosed" OR "I'm diagnosed") (depression OR "depressive disorder") lang:en -is:retweet"""
start_time = '2018-11-01T00:00:00Z' # inclusive
end_time = '2018-12-03T00:00:00Z' # exclusive
max_results = 500 # default
limit = 10 # default


client = getAuthentication(bearer_token)
response_lst = searchHistoricalTweets(client, query, start_time, end_time, max_results, limit)

if response_lst[0].meta['result_count'] > 0:
    processTweets(response_lst, 'resultados')
