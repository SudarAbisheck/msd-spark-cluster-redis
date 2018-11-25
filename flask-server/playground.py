import os
import logging
import pandas as pd
import json
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql.functions import col, date_format, rank, split, explode, collect_set, pandas_udf, PandasUDFType
from pyspark import SparkContext, SparkConf
from rediscluster import StrictRedisCluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUBMIT_ARGS = "--packages com.databricks:spark-csv_2.11:1.5.0 --jars /data/spark-redis-2.3.1-SNAPSHOT-jar-with-dependencies.jar pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

# load spark context
conf = SparkConf().setMaster("local[2]").setAppName("MSD_Application")
conf.set("spark.redis.host", "172.18.0.5")
conf.set("spark.redis.port", "6379")
# conf.set("spark.redis.auth", "passwd")

# Passing aditional Python modules to each worker
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

startup_nodes = [{"host": "172.18.0.5", "port": "6379"}]
rc = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)

# Loading data from data.CSV file
logger.info("Loading the data ....")
data = (
    sqlContext
    .read
    .format('com.databricks.spark.csv')
    .options(header='true', inferschema='true')
    .load('/data/data.csv')
)

logger.info("Cleaning the dataset ....")
# Choosing only the required columns
df = data.select('id', 'brand', 'colors', 'dateAdded') 
df = df.dropDuplicates(['id'])  # Dropping duplicate rows
df = df.dropna(how='any') # Dropping rows with atleast one null value


def __get_recent_items():
    window = Window.partitionBy(date_format(df.dateAdded, 'yyy-MM-dd')).orderBy(df['dateAdded'].desc())

    recent_items = (
        df.select('*', rank().over(window).alias('rank')) 
        .filter(col('rank') <= 1) 
        .dropDuplicates(['dateAdded']) # Sometimes, for a day, multiple products have been added at the recent time. Since the output has to be a single product, dropping the duplicates
        .orderBy(df.dateAdded, ascending=False)
    )

    # removing unnecessary fields
    recent_items = recent_items.select(date_format(df.dateAdded, 'yyy-MM-dd').alias('dateAdded'), 'id', 'brand','colors')

    # writing the results to redis (uses HASH data structure)
    recent_items.write \
            .format("org.apache.spark.sql.redis") \
            .option("table", "recent") \
            .option("key.column", "dateAdded") \
            .mode("overwrite") \
            .save()


@pandas_udf("dateAdded string, brandCount string", PandasUDFType.GROUPED_MAP)  # doctest: +SKIP
def brand_count_group(key, pdf):

    # tmp = concat_ws(':', pdf.brand, pdf.count).alias('brand:count')
    # tmp = pdf.brand.str.cat(pdf['count'].astype(str), sep=":")
    # tmp = '|'.join(tmp)
    # return pd.DataFrame([key +  (tmp,)])

    brand_count_list = [list(x) for x in zip(pdf["brand"],pdf["count"])]
    return pd.DataFrame([key + (str(brand_count_list),)])



def __count_brands_by_date_added():
    brand_counts = (
        df
        .groupBy([date_format(df.dateAdded, 'yyy-MM-dd').alias('dateAdded'), df.brand])
        .count()
        .orderBy(col("count").desc())
    )
    brand_counts = brand_counts.groupBy('dateAdded').apply(brand_count_group)

    brand_counts.write \
        .format("org.apache.spark.sql.redis") \
        .option("table", "count") \
        .option("key.column", "dateAdded") \
        .mode("overwrite") \
        .save()

    brand_counts.show()


### ---> This is the worst idea. The query takes for ever. Never gonna use it again. Falling back to lazy loading.
# def filter_by_color(color):
#     # print(type(color))
#     # print(color)
#     # return color
#     return str(df.filter(df.colors.contains(color)).orderBy(col('dateAdded').desc()).head(10))

# def __get_items_by_color_():

#     # temp = df.select('id', 'dateAdded', 'brand', split(df.colors, ',').alias('colors'))
#     # temp.show()

#     # temp.select('colors').distinct().show()

#     distinct_df = df.withColumn('colors', explode(split(col('colors'), ','))) \
#                     .agg(collect_set('colors').alias('distinct_colors'))
    
#     distinct_colors = distinct_df.select('distinct_colors').toPandas()['distinct_colors']

#     distinct_colors = distinct_colors.iloc[0]

#     temp = map(filter_by_color, distinct_colors)
#     print(temp)

def convertRowToJSON(row):
    m = row.getValuesMap(row.schema.fieldNames)
    json.dumps(m)

def __get_items_by_color(color):

    items = (
        # df.filter(df.colors.contains(color))
        # df.filter(df.colors.rlike("(?=[,-]?)(?![ ])(" + color + ")(?=[,-]?)(?![ ])"))
        df.withColumn('colors', split(col('colors'), ','))
        .withColumn('colorsExploded', explode(col('colors')))
        .where('colorsExploded == "' + color + '"')
        .orderBy(col('dateAdded').desc())
        .drop('colorsExploded')
        .limit(10)
        .toJSON()
        .map(lambda j: json.loads(j))
        .collect() # Needs python installed in worker too as we are using `json.loads`
    )
    # items = map(convertRowToJSON, items)

    # print(items)
    # items.write \
    #     .format("org.apache.spark.sql.redis") \
    #     .option("table", "color") \
    #     .option("key.column", "dateAdded") \
    #     .mode("overwrite") \
    #     .save()

    return items



def get_redis_data(color):
    # reading data from redis
    print("Data from Redis :")
    # print(rc.hgetall("count:2016-11-04"))

    if not color:
        return "[]"

    data = rc.get("color:" + color)

    if not data: 
        data = __get_items_by_color(color) 
        data = json.dumps(data)
        rc.set("color:" + color, data)
    
    # return data

    # data = {u'brandCount': u"Nine West:85|Nike:31|TOMS:27|Easy Spirit:18|G BY GUESS:15|Caterpillar:12|Bearpaw:10|MICHAEL Michael Kors:9|ELLIE SHOES:9|Propet:9|Dr. Scholl's:8|PUMA:6|SummitFashions:6|SKECHERS:6|Brinley Co.:6|MUK LUKS:5|VANS:5|Lucky Brand:5|White Mountain:5|PleaserUSA:4|Easy Spirit e360:4|SHOES OF SOUL:4|GUESS:4|Australia Luxe:4|Michael Antonio:4|Minnetonka:4|Stuart Weitzman:4|Marc Fisher:3|Dearfoams:3|Qupid:3|Jessica Simpson:3|Forever Collectible:3|Via Spiga:3|Alfani:3|Isotoner:3|Isaac Mizrahi:3|Anne Klein:3|Classique:3|Badgley Mischka:3|EasyComforts:3|Style & Co.:2|Naturalizer:2|Geox:2|Taryn Rose:2|Born:2|Sebastian Milano:2|New Balance:2|Pelle Moda:2|Penny Loves Kenny:2|Rock & Candy:2|Elie Tahari:2|Tretorn:2|Fidji:2|Allegra K:2|Bar III:2|Woolrich:2|Michael Kors:2|Unique Bargains:2|Sam Edelman:2|Bogs:2|El Naturalista:2|Beacon:2|Elites by Walking Cradles:2|kate spade:2|ShoeVibe:2|Grasshoppers:2|Fiel:2|J/Slides:2|Thalia Sodi:2|Donald J Pliner:2|Indigo Rd.:2|Vionic:2|Ugg Australia:2|Louise et Cie:2|Vince Camuto:2|Aldo:2|DAWGS:2|Seychelles:2|INC International Concepts:2|Callisto:2|Caparros:2|OH!:1|Under Armour:1|Bucco Capensis:1|Mia:1|Cole Haan:1|Karen Scott:1|Sanuk:1|Trina Turk:1|Camper Together:1|NINE WEST:1|Kenneth Cole Reaction:1|Jacobies:1|Santoni:1|JAMBU:1|Eric Michael:1|Calvin Klein:1|Derek Lam:1|Drew:1|HOKA ONE ONE:1|CAPE ROBBIN:1|Nomad:1|Chelsea Crew:1|Funtasma:1|Anne Michelle:1|Diba.True:1|Asics:1|Eileen Fisher:1|Easy USA:1|Praerie:1|Sorel:1|Loeffler Randall:1|Roxy:1|The Flexx:1|Pleaser:1|OKABASHI:1|Shellys London:1|Fergalicious:1|Studswar:1|Call It Spring:1|Fly London:1|Emu Australia:1|BCBGeneration:1|Dolce & Gabbana:1|Kim Rogers:1|Dolce by Mojo Moxy:1|Marc By Marc Jacobs:1|Elizabeth and James:1|Lauren By Ralph Lauren:1|SPRING:1|Aeropostale:1|Charles By Charles David:1|French Sole:1|Victoria K.:1|American Rag:1|LFL:1|Ugg:1|Dan Post:1|NCAA:1|Vision Street Wear:1|Alice & Olivia:1|BEAUTIFEET:1|Touch Ups:1|Lola Sabbia:1|Dansko:1|Unlisted Kenneth Cole:1|Rampage:1|Jambu:1|Spring Step:1|French Connection:1|Rebecca Minkoff:1|Mark Lemp By Walking Cradles:1|Coach:1|G.H. Bass & Co.:1|Boutique 9:1|Re-Sole:1|Taos:1|IMAN:1|Melissa:1|Aerosoles:1|Avanti:1|Calvin Klein Jeans:1|P&W New York:1|Cold Front:1|Dolce Vita:1|Matt Bernson:1|Ferrini:1|UNITED NUDE:1|Soda:1|Corral:1|Crocs:1|Vibram:1|Red Valentino:1|MLB:1|Lowa:1|Carlos Santana:1|Dana Davis:1|Everybody By BZ Moda:1|Diane von Furstenberg:1|Olivia Miller:1|Tory Burch:1|Cobb Hill by New Balance:1|NeoSport:1|Boreal:1|Manolo Blahnik:1|Easy Street:1|Reiker:1|Vasque:1|Luichiny:1|Modit:1|BareTraps:1|Gola:1|G.I.L.I.:1|Mossy Oak:1|Mephisto:1|Anne Klein Sport:1|Ami:1|Carole Hochman:1|Unbranded:1|Sesto Meucci:1|Helly Hansen:1|Faded Glory:1|Wanted:1|The North Face:1|Roces:1|Keen:1|Oka Bee:1|G Antini:1|Grazie:1|Ryka:1|Muck Boot:1|Miz Mooz:1|Nina:1|Adrienne Vittadini:1|TRUE linkswear:1|GX by Gwen Stefani:1|C Label:1|Sperry Top-Sider:1|Comfort Ease:1|FS/NY:1|Max Mara:1|Judith Ripka:1|G.C. Shoes:1|Norm Thompson:1|Reebok:1|Desigual:1"}
    # output = data['brandCount'].split('|')
    # output = map(lambda x: x.split(':'), output)
    # output = data['brandCount']
    print(data)
    # print(json.dumps(data))
    # print(output[0][0])


logger.info("Processing and storing necessary data in Redis ... ")
# __get_recent_items()
# __count_brands_by_date_added()
# __get_items_by_color('Blue')
get_redis_data('Blue')

# def test():
#     data = [['2016-11-04', 'Nine West', 85], ['2016-11-04', 'Nike', 31], ['2015-08-18','Ralph Lauren',40], ['2016-11-13','Nike',31], ['2016-11-13', 'TOMS', 27]]
#     testdf = pd.DataFrame(data, columns=['dateAdded', 'brand', 'count'])
    
#     # # tmp = testdf.brand.str.cat(testdf['count'].astype(str), sep=":")
#     # # print(tmp)
#     # # tmp = '|'.join(tmp)
#     # # print(tmp)

#     # # tmp = pd.Series(zip(testdf['brand'], testdf['count'])).tolist()
#     # tmp = pd.Series(testdf['count'].values, index=testdf['brand'].values).to_dict()
#     # tmp = pd.DataFrame([["mylist", str(tmp)]], columns=["ky", "data"])
#     # print(tmp)

#     # tmp_df = sqlContext.createDataFrame(tmp)
#     # tmp_df.write \
#     #     .format("org.apache.spark.sql.redis") \
#     #     .option("table", "test") \
#     #     .option("key.column", "ky") \
#     #     .mode("overwrite") \
#     #     .save()
#     # # print(tmp.iloc["0"])

#     # print(rc.hgetall("test:mylist"))

#     # # rc.hset("mylist", tmp[0][1])
#     # # print(rc.get("mylist"))
#     # # rc.delete('mylist')
#     # # rc.lpush('mylist', json.dumps(tmp))

#     # # print(rc.lrange('mylist', 0, -1))


#     ### third Test
#     tmp = pd.Series(zip(testdf['brand'], testdf['count'])).tolist()
#     tmp = pd.DataFrame([["mylist", str(tmp)]], columns=["ky", "data"])
#     tmp_df = sqlContext.createDataFrame(tmp)
#     tmp_df.write \
#         .format("org.apache.spark.sql.redis") \
#         .option("table", "test") \
#         .option("key.column", "ky") \
#         .mode("overwrite") \
#         .save()
#     print(rc.hgetall("test:mylist"))

# test()