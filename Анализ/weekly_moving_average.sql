SELECT 
	trip_day,
	moving_avg_week
FROM 
	(
		SELECT 
			*,
			AVG(trips_count) OVER (ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_week
		FROM 
			(
				SELECT arrayJoin(arrayMap(i -> (toDate('2008-01-01')+i), range(365*16))) AS cal_date
			) cal
			LEFT OUTER JOIN 
			nyc_tlc_data.daily_agg_data dad ON dad.trip_day = cal.cal_date
	)
WHERE trip_day = cal_date;