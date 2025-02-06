from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

#(key,value): (date,(station number, temp))
year_temperature = lines.map(lambda x: ((x[1][0:4],x[1][5:7],x[1][8:10],x[0]),(float(x[3]))))

#year_temperature.glom().collect()

#filter year between 1960 1nd 2014
year_temperature_filter =year_temperature.filter(lambda x: int(x[0][0])>= 1960 and int(x[0][0]) <= 2014)
#year_temperature_filter.glom().collect()

#Give max and min of each station in each date
#max_min_temperatures_daily= year_temperature_filter.reduceByKey(lambda x:(max(x),min(x)))
#max_min_temperatures_daily.glom().collect()
max_min_temperatures_daily=year_temperature_filter.groupByKey()
max_min_temperatures_daily = max_min_temperatures_daily.mapValues(lambda x: (max(x), min(x)))

#change the order
max_min_temp_count=max_min_temperatures_daily.map(lambda x: ((x[0][0], x[0][1], x[0][3]), (x[1][0] + x[1][1], 1)))
#max_min_temp_count.glom().collect()

#daily averaging in each station
daily_average= max_min_temp_count.reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1]))
#daily_average.glom().collect()

monthly_average = daily_average.map(lambda x: (x[0][0], x[0][1], x[0][2], (x[1][0]/(2*x[1][1]))))
monthly_average_sorted =monthly_average.sortBy(ascending = True, keyfunc=lambda k: (k[0]))
#monthly_average_sorted.glom().collect()

# save output to file
monthly_average_sorted=monthly_average_sorted.map(lambda x: ",".join(str(i) for i in x))\
               .saveAsTextFile("BDA/output")

########## tail part-00014:Year, month, station number, average monthly temperature
#2011,08,105220,13.7080645161
#2011,07,157860,16.4472222222
#2011,05,144310,5.2564516129
#2011,10,145580,2.94193548387
#2011,09,94180,12.5083333333
#2011,11,108320,5.56833333333
#2011,09,124110,8.68
#2011,08,145130,12.835483871
#2011,05,149340,6.76290322581
#2011,04,76420,8.81333333333
