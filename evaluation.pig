-- load data from prediction result
predictionData = LOAD 'part-r-00000' Using PigStorage('\t') AS (user_item:chararray, predicted:chararray);
prediction = FOREACH predictionData GENERATE user_item, (predicted matches '^NaN' ? 0:(double)predicted) as predicted;

--dump prediction;
--prediction = FILTER predictionResult BY predicted > 0;

-- load data from testing result
trueResult = LOAD 'TestingRatings.txt' USING PigStorage(',') AS (itemid:chararray,userid:chararray, trueRating:double);
newTrueResult = FOREACH trueResult{
	pair = CONCAT(itemid, ',', userid);
	GENERATE pair AS user_item, trueRating;
}

-- Join predictionResult with newTrueResult
join_result = JOIN prediction BY user_item, newTrueResult BY user_item USING 'replicated';
data = FOREACH join_result GENERATE prediction::user_item AS user_item, prediction::predicted AS predictRating, newTrueResult::trueRating AS trueRating;

-- Calculate MAE and MSE
mae_list = FOREACH data GENERATE user_item, ABS(predictRating - trueRating) AS mae;
--STORE mae_list INTO 'mae';
mse_list = FOREACH data GENERATE user_item, (predictRating - trueRating)*(predictRating - trueRating) AS mse;
--STORE mse_list INTO 'mse';

temp1 = GROUP mae_list ALL;
--dump temp1;
MAE = FOREACH temp1 GENERATE (SUM(mae_list.mae) / COUNT(mae_list.mae)) AS mae;


temp2 = GROUP mse_list ALL;
RMSE = FOREACH temp2 GENERATE SQRT(SUM(mse_list.mse)/COUNT(mse_list.mse)) AS rmseResult;
dump MAE
dump RMSE
