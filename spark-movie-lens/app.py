from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request
 
# 这个文件就是接口

# 预测用户user_id最可能喜欢的top count部电影
@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    return json.dumps(top_ratings)
 
# 预测用户user_id对电影movie_id的评分
@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings)
 
# 上传用户user_id的评分数据
@main.route("/<int:user_id>/ratings", methods = ["POST"])
def add_ratings(user_id):
    # 从Flask POST请求对象获得评分
    # ratings_list = request.form.keys()[0].strip().split("\n")
    ratings_list = request.form.get("key", type=str).strip().split("\n")
    ratings_list = map(lambda x: x.split(","), ratings_list)
    # 用engine（user_id，movie_id，rating）所需的格式创建一个列表
    ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
    # 将它们添加到使用引擎API的模型中    
    recommendation_engine.add_ratings(ratings)
 
    return json.dumps(ratings)
 
 
def create_app(spark_context, dataset_path):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app 
