from pyspark import SparkContext

# Set up SparkContext object
sc = SparkContext(appName = 'Lab1_Ex4')

# Upload data files
temps = sc.textFile('BDA/input/temperature-readings.csv')
precips = sc.textFile('BDA/input/precipitation-readings.csv')

# Split each line into a list of strings
temp_obs = temps.map(lambda line: line.split(";"))
precip_obs = precips.map(lambda line: line.split(";"))

# Create tuple with station as key and temp as value
station_temps = temp_obs.map(lambda x: (x[0], float(x[3])))

# Create tuple with (station, date) as key and precipitation as value
daily_precips = precip_obs.map(lambda x: ((x[0],x[1]), float(x[3])))

# Find and return max temperatures for each station
max_temps = station_temps.reduceByKey(lambda a, b: float(a) if float(a) >= float(b) else float(b))

# Find and return max daily precipitation for each station
daily_precips = daily_precips.reduceByKey(lambda a, b: float(a) + float(b))
max_precips = daily_precips.map(lambda x: (x[0][0], x[1]))
max_precips = max_precips.reduceByKey(lambda a, b: float(a) if float(a) >= float(b) else float(b))

# Combine the station data together
station_data = max_temps.join(max_precips)

# Filter out stations with max temperatures and precipitations outside the acceptable range
station_data = station_data.filter(lambda x: x[1][0] >= 25 and x[1][0] <= 30 and x[1][1] >= 100 and x[1][1] <= 200)

# Export the output in a text file
station_data.saveAsTextFile('BDA/output/BDA_Lab1_Ex4_Output')


### OUTPUT ###

# The output for this exercise was empty, as none of the stations met both of the filter criteria