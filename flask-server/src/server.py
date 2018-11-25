import os
import cherrypy
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf
import config as cfg

def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("MSD_Application")
    conf.set("spark.redis.host", cfg.redis['host'])
    conf.set("spark.redis.port", cfg.redis['port'])
    # conf.set("spark.redis.auth", "passwd")

    # Passing aditional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['src/engine.py', 'src/app.py', 'src/config.py'])
    return sc

def run_server(app):
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)
 
    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')

    # Set the configuration of the web server
    cherrypy.config.update(cfg.cherrypy)
 
    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()

if __name__ == "__main__":
    # Init spark context and load libraries
    sc = init_spark_context()
    dataset_path = '/data'
    app = create_app(sc, dataset_path)
 
    # start web server
    run_server(app)