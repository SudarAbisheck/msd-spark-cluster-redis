import os ,logging, json, ast
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql.functions import col, date_format, rank, explode, split
from rediscluster import StrictRedisCluster
# import udfs
import config as cfg

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkQueryEngine:

    def __init__(self, sc, dataset_path):
        logger.info("Starting Spark Query Engine..")

        self.sc = sc
        self.sqlContext = SQLContext(sc)

        startup_nodes = [{"host": cfg.redis['host'], "port": cfg.redis['port']}]
        self.rc = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)

        # Loading data from data.CSV file
        logger.info("Loading the data ....")
        data = (
                self.sqlContext
                .read
                .format('com.databricks.spark.csv')
                .options(header='true', inferschema='true')
                .load(
                    os.path.join(dataset_path,'data.csv')
                )
            )

        logger.info("Cleaning the dataset ....")
        self.df = data.select('id', 'brand', 'colors', 'dateAdded') # Choosing only the required columns
        self.df = self.df.dropDuplicates(['id'])  # Dropping duplicate rows
        self.df = self.df.dropna(how='any') # Dropping rows with atleast one null value

        logger.info("Processing and storing necessary data in Redis ... ")
        self.__get_recent_items()
        # self.__count_brands_by_date_added()


    def __get_recent_items(self):
        window = Window.partitionBy(date_format(self.df.dateAdded, 'yyy-MM-dd')).orderBy(self.df['dateAdded'].desc())

        recent_items = (
            self.df.select('*', rank().over(window).alias('rank')) 
            .filter(col('rank') <= 1) 
            .dropDuplicates(['dateAdded']) # For some dates, multiple products have been added at the recent time. Since the output has to be a single product, dropping the duplicates
            .orderBy(self.df.dateAdded, ascending=False)
        )

        # removing unnecessary fields
        recent_items = recent_items.select(date_format(self.df.dateAdded, 'yyy-MM-dd').alias('dateAdded'), 'id', 'brand', 'colors')

        # writing the results to redis (uses HASH data structure). Keys will be of format `recent:yyy-MM-dd`
        recent_items.write \
                .format("org.apache.spark.sql.redis") \
                .option("table", "recent") \
                .option("key.column", "dateAdded") \
                .mode("overwrite") \
                .save()

    def __count_brands_by_date_added(self):
        brand_counts = (
            self.df
            .groupBy([date_format(self.df.dateAdded, 'yyy-MM-dd').alias('dateAdded'), self.df.brand])
            .count()
            .orderBy(col("count").desc())
        )
        
        # @pandas_udf module expects the spark context to be present
        # when importing the udfs module. For some reason, the spark context is not available
        # if I add the import statement at the top and throws the error message 
        # AttributeError: 'NoneType' object has no attribute '_jvm'
        # https://github.com/apache/spark/blob/a09d5ba88680d07121ce94a4e68c3f42fc635f4f/python/pyspark/sql/types.py#L798-L806
        from udfs import brand_count_list_udf
        brand_counts = brand_counts.groupBy('dateAdded').apply(brand_count_list_udf)
    
        # writes the brand_counts dataframe to redis using hash datastructure. Keys will be of format `count:yyy-MM-dd`
        brand_counts.write \
            .format("org.apache.spark.sql.redis") \
            .option("table", "count") \
            .option("key.column", "dateAdded") \
            .mode("overwrite") \
            .save()


    def __get_items_by_color(self, color):
        items = (
            self.df
            .withColumn('colors', split(col('colors'), ',')) # split colors into an array
            .withColumn('colorsExploded', explode(col('colors')))
            .where('colorsExploded == "' + color + '"') # filter for the color
            .orderBy(col('dateAdded').desc())
            .drop('colorsExploded')
            .limit(10)
            .toJSON()
            .map(lambda j: json.loads(j))
            .collect() # Needs python installed in spark worker too as we are using `json.loads`
        )

        # Writes the data to redis (uses SET data structure). Key is of format `color:str.lower(color)`
        output = json.dumps(items)
        self.rc.set("color:" + color.lower(), output)
        return output

    def get_recent_item_api(self, dateString):
        """ Gets the recent item given the date and returns as output as json"""
        output = self.rc.hgetall("recent:" + str(dateString))
        if not output: 
            return json.dumps(output)
        else:
            output["colors"] = output["colors"].split(",")
            return json.dumps(output)


    def get_brands_count_api(self, dateString):
        """ Gets the brand counts for a particular date and returns as output as json"""
        output = self.rc.hgetall("count:" + str(dateString))
        return json.dumps(ast.literal_eval(output['brandCount']))

    def get_items_by_color_api(self, color):
        """ 
            Lazy loads the items given a color.
            If the data is not present in redis, queries spark to collect the data and store it in redis.
        """
        if not color:
            return "[]"

        output = self.rc.get("color:" + color.lower())

        if not output: 
            output = self.__get_items_by_color(color) 

        return output

