-- Load data from TestingRatings.txt
user_data = LOAD 'netflix_subset/TestingRatings.txt' USING PigStorage(',') AS (movieId:chararray, userId:chararray, rating:int); 

/*
-- Load data from TrainingRatins.txt
user_data = LOAD 'netflix_subset/TrainingRatings.txt' USING PigStorage(',') AS (movieId:chararray, userId:chararray, rating:int); 
*/

-- count for distinct users
users = FOREACH user_data GENERATE userId;
uniq_users = DISTINCT users; 
grouped_users = GROUP uniq_users ALL;
uniq_user_count = FOREACH grouped_users GENERATE COUNT(uniq_users);
DUMP uniq_user_count;

-- count for distinct movies
movies = FOREACH user_data GENERATE movieId;
uniq_movies = DISTINCT movies;
grouped_movies = GROUP uniq_movies ALL;
uniq_movie_count = FOREACH grouped_movies GENERATE COUNT(uniq_movies);
DUMP uniq_movie_count;





                                     
            
        

                        
                                     


