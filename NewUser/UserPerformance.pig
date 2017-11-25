predictionData = LOAD 'part-r-00000' Using PigStorage('\t') AS (item_user:chararray, predicted:chararray);
prediction = FOREACH predictionData GENERATE FLATTEN(STRSPLIT(item_user,',')) as (item:chararray,user:chararray), (predicted matches '^NaN' ? 0:(double)predicted) as predicted;
prediction_filter0 = FILTER prediction by predicted>0;

movie_title = LOAD 'movie_titles.txt' Using PigStorage(',') AS (item:chararray, year:chararray,title:chararray);
join_title = JOIN prediction_filter0 BY item, movie_title BY item USING 'replicated';
result_title = FOREACH join_title GENERATE prediction_filter0::user AS user,movie_title::title AS title, prediction_filter0::predicted AS predicted; 

group_by_user = GROUP result_title BY user;
result = FOREACH group_by_user  {
by_order = ORDER result_title BY predicted DESC;
by_filter = limit by_order 10;
GENERATE group as user, by_filter as prediction_group;
}
dump result;


