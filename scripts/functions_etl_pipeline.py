# Seamless API interactions
import requests
# Interacting with MongoDB databases
from pymongo import MongoClient
# Converting the string into an ObjectId
from bson import ObjectId
# Progress bar for tracking progress 
from progress.bar import Bar

####### Extracting #######
def extract_data():
    # Connecting to the MongoDB server
    conn = MongoClient('mongodb://localhost:27017')
    
    # Accessing to the target database
    db_conn = conn['TheMovieDB']
    
    # Accessing to the trackers collection
    cl_Trackers = db_conn['Trackers']
    
    # Retrieving the last processed batch
    last_processed_batch = 0
    if cl_Trackers.count_documents({}) > 0:
        trackers = cl_Trackers.find().sort('_id',-1).limit(1)
        for tracker in trackers:
            last_processed_batch = tracker.get('last_processed_batch')
    
    # Closing the connection
    conn.close()
    
    # Replacing with my API key
    API_KEY = "4cdf240ad484234c52f16bb1fb68601b"
    BASE_URL = "https://api.themoviedb.org/3"
    
    # Initializing lists to store raw data from the REST API
    ## To store raw movie data
    raw_movies = [] 
    ## To store raw cast data
    raw_casts = []
    ## To store raw crew data
    raw_crews = []
    
    # Defining the start and end indices for the range
    ## Defining the start index
    start_index = 1
    if last_processed_batch >= 0:
        start_index = last_processed_batch + 1
    
    ## Defining the end index
    end_index = start_index + 1000
    
    ## Specifying the last processed batch for the next run
    last_processed_batch = end_index - 1
    
    # Creating a progress bar with a custom prefix and suffix
    bar = Bar('Processing', max=(end_index - 1), suffix='%(percent)d%%')
    
    # Begining data retrieval
    for i in range(start_index, end_index):
        ## Defining the params
        url_movie = f"{BASE_URL}/movie/{i+1}"
        url_cast_crew = f"{BASE_URL}/movie/{i+1}/credits"
    
        params = {
            "api_key": API_KEY
        }
        
        ## Extracting data from the API
        response_movie = requests.get(url_movie, params=params)
        
        ## Checking if the content exists
        if response_movie.status_code == 200:
            ### Extracting the data for movies in JSON format
            data_movie = response_movie.json()
            
            ### Extracting data from the API
            response_cast_crew = requests.get(url_cast_crew, params=params)
    
            ### Extracting the data for casts and crews in JSON format
            data_cast_crew = response_cast_crew.json()
            data_cast = data_cast_crew.get("cast",[])
            data_crew = data_cast_crew.get("crew",[])
            
            ### Appending the extracted data to the raw lists
            raw_movies.append(data_movie)
            raw_casts.append(data_cast)
            raw_crews.append(data_crew)
        
        ## Move the progress bar forward
        bar.next()
        
    return raw_movies, raw_casts, raw_crews, last_processed_batch

####### Transforming #######
def transform_data(raw_movies, raw_casts, raw_crews):
    # Initializing lists to store relevant data
    ## To store movies
    list_movies = []
    ## To store casts
    list_casts = []
    ## To store crews
    list_crews = []
    
    # Defining the total number of steps
    total_steps = len(raw_movies)
    
    # Creating a progress bar with a custom prefix and suffix
    bar = Bar('Processing', max=total_steps, suffix='%(percent)d%%')
    
    # Begining data extracting
    for i in range(0, total_steps):
        ## Extracting the relevant data for movies
        title = raw_movies[i].get("title")
        release_date = raw_movies[i].get("release_date")
        genres = raw_movies[i].get("genres", "[]")
        genres = [genre.get("name") for genre in genres]
        overview = raw_movies[i].get("overview")
        popularity = raw_movies[i].get("popularity")
        rating = raw_movies[i].get("vote_average")
        poster_url = raw_movies[i].get("homepage")
        
        ## Appending the extracted data to the movies list
        list_movies.append({"title" : title, "release_date" : release_date, "genres" : genres, "overview" : overview, "popularity" : popularity, "rating" : rating, "poster_url" : poster_url})
    
        ## Extracting the relevant data for casts
        raw_cast = raw_casts[i]
        casts = []
        for j in range(0, len(raw_cast)):
            actor_name = raw_cast[j].get("name")
            character_name = raw_cast[j].get("character")
            order = raw_cast[j].get("order")
            casts.append({"actor_name" : actor_name, "character_name" : character_name, "order" : order})
        
        ## Appending the extracted data to the casts list
        list_casts.append(casts)

        ## Extracting the relevant data for crews    
        raw_crew = raw_crews[i]
        crews = []
        for j in range(0, len(raw_crew)):
            name = raw_crew[j].get("name")
            role = raw_crew[j].get("job")
            profile_url = raw_crew[j].get("profile_path")
            crews.append({"name" : name, "role" : role, "profile_url" : profile_url})
        
        ## Appending the extracted data to the casts list            
        list_crews.append(crews)
        
        ## Move the progress bar forward
        bar.next()
        
    return list_movies, list_casts, list_crews

####### Loading #######
def load_data(list_movies, list_casts, list_crews, last_processed_batch):
    # Connecting to the MongoDB server
    conn = MongoClient('mongodb://localhost:27017')
    
    # Accessing to the target database
    db_conn = conn['TheMovieDB']
    
    # Accessing to all collections
    cl_Movies = db_conn['Movies']
    cl_Casts = db_conn['Casts']
    cl_Crews = db_conn['Crews']
    cl_Trackers = db_conn['Trackers']
    
    # Defining the total number of steps
    total_steps = len(list_movies)
    
    # Creating a progress bar with a custom prefix and suffix
    bar = Bar('Processing', max=total_steps, suffix='%(percent)d%%')
    
    # Begining Loading
    for i in range(0, total_steps):
        ## Inserting the movie
        result = cl_Movies.insert_one(list_movies[i])
        movie_id = result.inserted_id
        
        ## Inserting the casts related to the current movie
        list_cast = list_casts[i]
        for j in range(0, len(list_cast)):
            list_cast[j]["movie_id"] = ObjectId(movie_id)
            cl_Casts.insert_one(list_cast[j])
        
        ## Inserting the casts related to the current movie
        list_crew = list_crews[i]
        for j in range(0, len(list_crew)):
            list_crew[j]["movie_id"] = ObjectId(movie_id)
            cl_Crews.insert_one(list_crew[j])
        
        ## Move the progress bar forward
        bar.next()
        
    # Inserting the last processed batch for the next run
    cl_Trackers.insert_one({"last_processed_batch" : last_processed_batch})
    
    # Closing the connection
    conn.close()