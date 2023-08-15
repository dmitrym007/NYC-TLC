SELECT 
	rankCorr(fare_amount_avg, prec_cm) AS pay_prec_corr,
	rankCorr(fare_amount_avg, avg_wind_ms) AS pay_wind_corr,
	rankCorr(fare_amount_avg, snowfall_mm) AS pay_snowfall_corr,
	rankCorr(fare_amount_avg, snow_depth_mm) AS pay_snow_depth_corr,
	rankCorr(fare_amount_avg, max_temperature_tenth_celsius) AS pay_max_temp_corr,
	rankCorr(fare_amount_avg, min_temperature_tenth_celsius) AS pay_min_temp_corr
FROM
	(
		SELECT 
			dwc.*,
			dad.fare_amount_avg
		FROM 
			nyc_tlc_data.daily_agg_data dad
		JOIN 
			nyc_tlc_data.daily_weather_conditions dwc ON dad.trip_day = dwc.observation_date	
);