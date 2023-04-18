import pandas as pd
import tweepy
import time
import datetime
import os


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
def getUserTimeline(client, userID, limit=32):

    # Returns Tweets composed by a single user, specified by the requested user ID.
    # By default, the most recent ten Tweets are returned per request.

    print(f'\nBuscando tweets: {userID}')

    response_lst = []

    # Obtener los primeros 3200 tweets sin retweets (o sea pueden ser menos de 3,200)
    # limit = 32 porque son 32 llamadas máximas al API para obtener 3,200 (32 llamadas de 100 tweet cada una)
    for response in tweepy.Paginator(client.get_users_tweets,
                                    id=userID,
                                    exclude=['retweets'],
                                    expansions='geo.place_id,author_id',
                                    place_fields=['full_name', 'id', 'country', 'country_code', 'geo', 'name', 'place_type'],
                                    tweet_fields=['author_id', 'created_at', 'lang', 'possibly_sensitive', 'public_metrics'],
                                    user_fields=['id', 'username', 'description', 'location', 'profile_image_url', 'verified', 'public_metrics'],
                                    max_results=100, # Máximo regresado por el API
                                    limit=limit):
        
        # Sleep 2 seconds
        time.sleep(2)

        print(response.meta)

        # Append raw response - Best practice to handle errors, etc.
        response_lst.append(response)
    

    return response_lst

## ==============================
## Step 3. Procesar Tweets
## ==============================
def processTweets(response_lst, destiny_directory):

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
                                    'user_followers_count': user.public_metrics['followers_count'],
                                    'user_friends_count': user.public_metrics['following_count'],
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
                'user_followers_count': author_info['user_followers_count'],
                'user_friends_count': author_info['user_friends_count'],
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

    # Exportarlo a csv
    user_name = df['user_username'][0]
    print(f'User: {user_name}\t{userID}\t{df.shape[0]}')
    
    print(f'Guardando :)')
    filename = f'{user_name}_{userID}.csv'
    df.to_csv(os.path.join(destiny_directory, filename), index=False)





# ======================================================================
bearer_token = ''

client = getAuthentication(bearer_token)


now = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')
print(f'\nStart: {now}...\n')

# Directorio para guardar  los timelines
directory = r'Timeline_users'
LIMIT_TIMELINES = 32

# Archivo csv con los ids de los usuarios a buscar
users_retrieve_list = pd.read_csv(r'ids_search_timelines.csv', dtype={'user_id': str})['user_id'].values.tolist()
total_users = len(users_retrieve_list)

print(f'Total de usuarios = {total_users}\n')

preprocessed_users = []

# Iterar sobre todos los usuarios a buscar
for i, userID in enumerate(users_retrieve_list, start=1):

    # Buscar timeline
    response_timeline = getUserTimeline(client, userID, LIMIT_TIMELINES)

    # Procesar segun la respuesta
    if response_timeline[0].data:
        processTweets(response_timeline, directory)
        preprocessed_users.append([userID, 'successful'])
    
    elif len(response_timeline[0].errors) > 0:
        error = response_timeline[0].errors[0]['detail']

        print(f'Error en usuario {userID}, {error}')
        preprocessed_users.append([userID, error])
    
    elif response_timeline[0].meta:
        count = response_timeline[0].meta['result_count']

        print(f'Usuario {userID} con {count} tweets')
        preprocessed_users.append([userID, count])


    if i % 10 == 0:
        now = datetime.today().strftime('%d-%m-%Y %H:%M:%S')
        print(f'-> Procesados {i}/{total_users} - {now} <-')


print('\n\n---> FIN!!! :) <---')


# Status final de los usuarios, si hubo algún error, o así...
status_usuarios = pd.DataFrame(data=preprocessed_users, columns=['user_id', 'status'])
date_time = datetime.datetime.now().strftime('%d%m%y_%H%M%S%f') # Concatena al final fecha y hora
status_usuarios.to_csv(f'status_usuarios_{date_time}.csv', index=False)
