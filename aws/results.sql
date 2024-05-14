
--1. Which car make is more efficient?
SELECT make, AVG(electric_range) as average_range
FROM processed_data
WHERE ev_type = 'Plug-in Hybrid Electric Vehicle (PHEV)'  -- Battery Electric Vehicle (BEV), Plug-in Hybrid Electric Vehicle (PHEV) Focusing on Battery Electric Vehicles for efficiency
GROUP BY make
ORDER BY average_range DESC;

--2. Is there any relationship between the choice of EV make and city?
SELECT city, make, COUNT(*) as total
FROM processed_data
GROUP BY city, make
ORDER BY total DESC;

--Which Plug-in Hybrid Electric Vehicle (PHEV) is preferred by buyers?
SELECT make, model, COUNT(*) as total_count
FROM processed_data
WHERE ev_type = 'Plug-in Hybrid Electric Vehicle (PHEV)'
GROUP BY make, model
ORDER BY total_count DESC

--Based on the data, which car make and model would you recommend?
SELECT make, model, AVG(electric_range) as average_range, COUNT(*) as total_count
FROM processed_data
GROUP BY make, model
ORDER BY average_range DESC, total_count DESC
