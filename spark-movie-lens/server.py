import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf

def init_spark_context():
    # spark context
    conf = SparkConf().setAppName("movie_recommendation-server")
    # 将上下文传给engine和app
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])
 
    return sc
 
 
def run_server(app):
 
    # 启用WSGI
    app_logged = TransLogger(app)
 
    # 将WSGI可调用对象（app）装载到根目录中
    cherrypy.tree.graft(app_logged, '/')
 
    # 配置web服务器
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5440,
        'server.socket_host': '0.0.0.0'
    })
 
    # 启动CherryPy WSGI Web服务器
    cherrypy.engine.start()
    cherrypy.engine.block()
 
 
if __name__ == "__main__":
    # 初始化spark context并加载库
    # 注意文件路径
    sc = init_spark_context()
    dataset_path = r'C:\Programs\spark-movie-lens\datasets\ml-latest-small'
    app = create_app(sc, dataset_path)
 
    # start web server
    run_server(app)

