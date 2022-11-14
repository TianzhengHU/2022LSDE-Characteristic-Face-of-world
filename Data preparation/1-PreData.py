# Databricks notebook source
# display(dbutils.fs.ls ("/mnt/lsde/"))
# file path: dbfs:/mnt/lsde/datasets/flickr/yfcc100m_dataset-0.bz2
# group path: /dbfs/mnt/lsde/group08

%pip install reverse_geocoder
%pip install country_converter

# COMMAND ----------

# below here is the map method, build a world map grid 18x36 
import country_converter as coco
import reverse_geocoder as rg

# create a new earth grid ,should be [lat(-90,90)][lon(-180,180)]
# grid [lat(0,180)][lon(0,360)]
latrange = 180
lats = -int(latrange/2)
latend = int(latrange/2)
lonrange = 360
lons = -int(lonrange/2)
lonend = int(lonrange/2)
print(lats)
print(type(lats))
totalsize = lonrange*latrange
print(totalsize)

countryGrid  = [[0 for lon in range(2)] for lat in range(totalsize)]
collon = 0
for lon in range(lons,lonend):
    for lat in range(lats,latend):
        countryGrid[collon][0] = lon
        countryGrid[collon][1] = lat
        collon = collon+1
    print(str(lon))
    print()

# COMMAND ----------

print(countryGrid)

# COMMAND ----------

# counvert the 2d list to dataframe
def get_host(item):
    lat = item[1]
    lon = item[0]
    loc = rg.search((lat,lon))
    country=loc[0]['cc']#国家
#       countryName = [country]
    return (lon,lat,country)
rdd = sc.parallelize(countryGrid)
countryGridDf = rdd.map(get_host).toDF()
countryGridDf = countryGridDf.toDF('lon','lat','country')#,'continent'
display(countryGridDf)

# COMMAND ----------

# write my map matrix into parquet file --->for save it
countryGridDf.repartition("country").write.mode("overwrite").partitionBy("country").parquet("/mnt/lsde/group08/Country_dictionary/MapMatrix180_360.parquet")


# COMMAND ----------

Map = spark.read.parquet("/mnt/lsde/group08/Country_dictionary/MapMatrix180_360.parquet")

Map.display()
Map.createOrReplaceTempView("Map")

# COMMAND ----------

print(Map.count())

# COMMAND ----------

from pyspark.sql import Row
import pyspark.sql.functions as func

countrylist = Map.groupBy(Map[2]).agg(func.collect_list('country').alias("country_list"))
countrylist.display()

# COMMAND ----------

countrylist_count = Map.groupBy(Map[2]).count()
countrylist_count.display()

# COMMAND ----------

country_number = 236
print(type(countrylist_count))
# print(type(country_number))

# COMMAND ----------

all_country_name = countrylist_count.select(countrylist_count[0])
all_country_name.display()

# COMMAND ----------

all_country_name_list = all_country_name.head(country_number)


# COMMAND ----------

print(type(all_country_name_list))
print(all_country_name_list)

# COMMAND ----------

con_list = []
for i in range(country_number):
    con_list.append(all_country_name_list[i]['country'])
    
print(len(con_list))
print(con_list)

# COMMAND ----------

import country_converter as coco
continent_dic = coco.convert(names=con_list, src = 'ISO2',to='name_short')

print(continent_dic)

# COMMAND ----------

from pyspark.sql import Row

Allcountry = countrylist_count.select(countrylist_count[0])
Allcountry.display()
print(type(Allcountry.head(country_number)))



# COMMAND ----------

all_country_list = Allcountry.head(country_number)
print(all_country_list)

# COMMAND ----------

import country_converter as coco

countries_list = []
for i in range(country_number):
    a_n = all_country_list[i]['country']
    countryName = [a_n]
    continent = coco.convert(names=countryName, src = 'ISO2',to='continent')
    print(a_n)
    print(continent)
    countries_list.append((a_n,continent))
    
print(countries_list)

# COMMAND ----------

# counvert the continents list to dataframe
def get_host(item):
    return (item)
rdd = sc.parallelize(countries_list)
countriesDf = rdd.map(get_host).toDF()
continentDf = countriesDf.toDF('country','continent')
display(continentDf)

# COMMAND ----------

whole_map = Map.join(continentDf,"country")
whole_map.show()

# COMMAND ----------



# COMMAND ----------

flickrs = spark.sql("select * from flickr where lat IS NOT NULL")
print(flickrs.count())
flickrs.show()


# COMMAND ----------

import pyspark.sql.functions as func

int_gps = flickrs.select(func.round((flickrs[4]), 2).cast('integer').alias("lon"),func.round((flickrs[5]), 2).cast('integer').alias("lat"),flickrs[1],flickrs[2])
int_gps.display()

# COMMAND ----------

int_gps.count()

# COMMAND ----------

# join the two table
joinTable = int_gps.join(whole_map).filter(int_gps[0] == whole_map[1]).filter(int_gps[1] == whole_map[2])
joinTable.display()

# COMMAND ----------

info_table = joinTable.select(joinTable[2],joinTable[3],joinTable[4],joinTable[7])
info_table.display()

# COMMAND ----------

info_table.repartition("continent","country").write.mode("overwrite").partitionBy("continent","country").parquet("/mnt/lsde/group08/Country_dictionary/SpatialInfo.parquet")

# COMMAND ----------

info_table.groupBy(info_table[2]).count().display()

# COMMAND ----------

info_table.groupBy(info_table[3]).count().display()

# COMMAND ----------

table_collect = info_table.groupBy(info_table[2]).agg(func.first(info_table[3]).alias("continent"),func.collect_list('url').alias("url_list"),func.collect_list('path').alias("path_list"))

# COMMAND ----------

country_number = table_collect.count()
print(country_number)

# COMMAND ----------

url_collect_list = table_collect.head(2)
print(url_collect_list)

# COMMAND ----------

a_countey_urls = url_collect_list[1]['url_list']
a_countey_paths = url_collect_list[1]['path_list']
a_countey_Name = url_collect_list[1]['country']
print(len(a_countey_urls))
print(type(a_countey_urls))
print(a_countey_Name)
print(type(a_countey_Name))
print(len(a_countey_paths))
print(type(a_countey_paths))

# COMMAND ----------


print("running")

# COMMAND ----------

display(dbutils.fs.ls ("/mnt/lsde/group08"))

# COMMAND ----------

display(dbutils.fs.ls ("/mnt/lsde/group08/Country_dictionary/SpatialInfo.parquet/"))
