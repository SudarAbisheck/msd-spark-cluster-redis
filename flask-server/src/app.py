from flask import Flask, request, Blueprint
from engine import SparkQueryEngine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

main = Blueprint('main', __name__)

@main.route('/', methods=["GET"])
def get_hello():
    logger.debug("Inside /")
    return 'OK! Looks like its working.'

@main.route('/getRecentItem/<string:date>', methods=["GET"])
def get_recent_item(date):
    """Returns the most recent item added on the given date."""
    logger.debug("Requested the recent item added on %s", date)
    return spark_query_engine.get_recent_item_api(date)

@main.route('/getBrandsCount/<string:date>', methods=["GET"])
def get_brands_count(date):
    """Returns the count of each brands added on the given date in descending order"""
    logger.debug("Requested for count of each brands added on %s", date)
    return spark_query_engine.get_brands_count_api(date)

@main.route('/getItemsbyColor/<string:color>', methods=['GET'])
def get_items_by_color(color):
    """Returns the top 10 latest items given input as color."""
    logger.debug("Requested for the top 10 latest items with the color %s", color)
    return spark_query_engine.get_items_by_color_api(color)

def create_app(spark_context, dataset_path):
    global spark_query_engine

    spark_query_engine = SparkQueryEngine(spark_context, dataset_path)    
    
    # Using the Blueprint to intialize Flask
    app = Flask(__name__)
    app.register_blueprint(main)
    return app
