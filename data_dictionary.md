# Fact Table


## `fact_immigration` 

> Gives us the main facts that can be used for queries. Below are the columns in the fact table:

* `arrival_date`: Arrival date of immigrant 
* `departure_date`: Departure date of immigrant
* `arrival_port`:  Arrival port (3 character code)
* `city_of_origin`: City immigrated from (2 digit code)
* `country_of_origin`: Country immigrated from
* `mode_of_travel`: How immigration occurred (1 digit code or null)
* `age`: Age of immigrant (years old)
* `visa_type`: Visa code (1 digit: Business/Pleasure/Student)
* `AveargeTemperature`: Average temperature of city


# Dimension Tables

## `dim_immigration`

> Provides some of the relevant information from the i94 dataste.

Columns:
* `arrival_date`: Arrival date of immigrant 
* `departure_date`: Departure date of immigrant
* `arrival_port`:  Arrival port (3 character code)
* `city_of_origin`: City immigrated from (2 digit code)
* `country_of_origin`: Country immigrated from
* `mode_of_travel`: How immigration occurred (1 digit code or null)
* `age`: Age of immigrant (years old)
* `visa_type`: Visa code (1 digit: Business/Pleasure/Student)

## `dim_temperature` 

> Provides the columns from the global temperature dataset.

Columns:
* `port`: i94 code of port for city
* `City`: Name of city
* `Country`: Name of city's country
* `AverageTemperature`: Average temperature of city
* `Latitude`: Latitude of city
* `Longitude` Longitude of city
