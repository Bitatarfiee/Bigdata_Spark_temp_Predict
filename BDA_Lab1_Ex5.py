from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")

#reading precipitation data
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
precipitation = precipitation_file.map(lambda line: line.split(";"))

#reading station stations-Ostergotland output : python list of all Ostergotland_station
stations_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
Ostergotland_station = stations_file.map(lambda line: line.split(";"))
Ostergotland_station = Ostergotland_station.map(lambda x: (x[0]))
Ostergotland_station = Ostergotland_station.collect()


#(key,value): (station number, year. month, date,precipitation)
#precipitation = precipitation.map(lambda x: (x[0],x[1][0:4],x[1][5:7],x[1][8:10],float(x[3])))
precipitation = precipitation.map(lambda x: (x[0],x[1][0:4],x[1][5:7],float(x[3])))
#filter year between 1993 1nd 2016 and Ostergotland_station
year_precipitation_filter =precipitation.filter(lambda x: int(x[1])>= 1993 and int(x[1]) <= 2016 and x[0] in Ostergotland_station)

#change the order key(station number,year,month) value(precipitation,count)
year_precipitation_1=year_precipitation_filter.map(lambda x: ((x[0], x[1], x[2]),(x[3])))
#calculate the total monthly precipitation for each station
year_precipitation_sum_station_month= year_precipitation_1.reduceByKey(lambda x, y: (x+y))

#total monthly precipitation for each station before calculating the monthly average (by averaging over stations)
year_precipitation_2=year_precipitation_sum_station_month.map(lambda x: ((x[0][1], x[0][2]),(x[1],1)))
monthly_precipitation_average_over_station= year_precipitation_2.reduceByKey(lambda x, y: ((x[0]+y[0]),(x[1]+y[1])))
final_average=monthly_precipitation_average_over_station.map(lambda x: ((x[0][0], x[0][1]),(x[1][0]/x[1][1])))
final_average=final_average.sortBy(ascending = True, keyfunc=lambda k: (k[0]))
#Ostergotland_station=Ostergotland_station.saveAsTextFile("BDA/output")
final_average=final_average.map(lambda x: ",".join(str(i) for i in x))\
               .saveAsTextFile("BDA/output")


#tail -n 10 part-00003 Year, month, average monthly precipitation
#(u'2011', u'06'),88.35
#(u'2011', u'07'),94.9166666667
#(u'2011', u'08'),86.2666666667
#(u'2011', u'09'),52.5666666667
#(u'2011', u'10'),43.75
#(u'2011', u'11'),13.4666666667
#(u'2011', u'12'),42.1333333333
#(u'2012', u'01'),43.55
#(u'2012', u'02'),28.6666666667
#(u'2012', u'03'),8.55
