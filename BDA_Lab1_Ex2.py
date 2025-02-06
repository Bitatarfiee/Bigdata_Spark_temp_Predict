from pyspark import SparkContext

# Set up SparkContext object
sc = SparkContext(appName = 'Lab1_Ex2')

# Upload data file
temps = sc.textFile('BDA/input/temperature-readings.csv')

# Split each line into a list of strings
obs = temps.map(lambda line: line.split(";"))

# Create tuple with year as key and temperature as value
# Part 1: All observations
# month_year_temps = obs.map(lambda x: (x[1][0:7], float(x[3])))
# Part 2: Individual stations
month_year_temps = obs.map(lambda x: (x[1][0:7], (x[0], float(x[3]))))

# Filter out observations before 1950, after 2014, or with temperatures below 10
month_year_temps = month_year_temps.filter(lambda x: int(x[0][0:4]) >= 1950 and int(x[0][0:4]) <= 2014 and x[1][1] >= 10).cache() # Extra index is added for part 2

# Part 2 only: Find unique month/station combinations
month_year_temps = month_year_temps.map(lambda x: (x[0], x[1][0]))
month_year_temps = month_year_temps.distinct()

# Group observations by month and date and return the count for each
month_year_counts = month_year_temps.mapValues(lambda x: 1)
month_year_counts = month_year_counts.reduceByKey(lambda a, b: a + b)

# Export the output in a text file
month_year_counts.saveAsTextFile('BDA/output/BDA_Lab1_Ex2_Output')


### OUTPUT ###
"""
Part 1 Output (All Observations) - First 20 Rows
------------------------------------------------
Month (YYYY-MM); Occurences 
(u'1981-03', 428)
(u'1974-07', 67336)
(u'2003-05', 49447)
(u'2004-04', 14868)
(u'1981-10', 10455)
(u'1987-05', 17996)
(u'2009-07', 133570)
(u'1986-11', 1516)
(u'1966-08', 52147)
(u'1952-03', 2)
(u'1978-03', 341)
(u'1977-06', 52848)
(u'1955-11', 89)
(u'1994-09', 34894)
(u'1978-10', 13057)
(u'2005-03', 1367)
(u'1950-09', 3791)
(u'1980-04', 5242)
(u'2007-09', 62572)
(u'1967-07', 54385)
"""

"""
Part 2 Output (Individual Stations) - First 20 Rows
---------------------------------------------------
Month (YYYY-MM); Occurences 
(u'1997-04', 194)
(u'1974-07', 362)
(u'2003-05', 321)
(u'1981-10', 329)
(u'1983-09', 332)
(u'1987-05', 321)
(u'1979-04', 228)
(u'2009-07', 312)
(u'1986-11', 151)
(u'1966-08', 359)
(u'1952-03', 2)
(u'1950-09', 50)
(u'1978-03', 108)
(u'1955-11', 26)
(u'1994-09', 299)
(u'1952-10', 67)
(u'1996-03', 3)
(u'2005-03', 132)
(u'1978-10', 341)
(u'1980-04', 265)
"""