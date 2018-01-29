'''
推荐算法引擎，封装了spark操作
'''
import os
import logging
from pyspark.mllib.recommendation import ALS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_counts_and_averages(ID_and_ratings_tuple):
    """给一个tuple (movieID, ratings_iterable) 
    返回 (movieID, (ratings_count, ratings_avg))
    """
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)


class RecommendationEngine:
    """
    电影推荐引擎
    """

    def __count_and_average_ratings(self):
        """
        根据当前self.ratings_RDD更新电影的评分
        """
        logger.info("Counting movie ratings...")
        movie_ID_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(get_counts_and_averages)
        self.movies_rating_counts_RDD = movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))


    def __train_model(self):
        """
        训练ALS模型
        """
        logger.info("Training the ALS model...")
        self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed,
                               iterations=self.iterations, lambda_=self.regularization_parameter)
        logger.info("ALS model built!")


    def __predict_ratings(self, user_and_movie_RDD):
        """
        预测电影评分，输入(userID, movieID)数据集，返回(movieTitle, movieRating, numRatings)
        """
        predicted_RDD = self.model.predictAll(user_and_movie_RDD)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.movies_titles_RDD).join(self.movies_rating_counts_RDD)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
        
        return predicted_rating_title_and_count_RDD
    
    def add_ratings(self, ratings):
        """
        添加新的电影打分  (user_id, movie_id, rating)
        """
        # 把评分转换成RDD
        new_ratings_RDD = self.sc.parallelize(ratings)
        # 为现有的评分添加新的评分
        self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
        # 重新计算电影评分
        self.__count_and_average_ratings()
        # 用新的评分重新训练ALS模型
        self.__train_model()
        
        return ratings

    def get_ratings_for_movie_ids(self, user_id, movie_ids):
        """
        输入一个用户ID和一个电影ID列表，预测用户对每部电影的打分
        """
        requested_movies_RDD = self.sc.parallelize(movie_ids).map(lambda x: (user_id, x))
        # 获得预测评分
        ratings = self.__predict_ratings(requested_movies_RDD).collect()

        return ratings
    
    def get_top_ratings(self, user_id, movies_count):
        """
        返回最值得推荐的movies_count部电影
        """
        # 获取该ID未评级的(userID, movieID) 对
        user_unrated_movies_RDD = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id)\
                                                 .map(lambda x: (user_id, x[1])).distinct()
        # 获得预测评分
        ratings = self.__predict_ratings(user_unrated_movies_RDD).filter(lambda r: r[2]>=25).takeOrdered(movies_count, key=lambda x: -x[1])

        return ratings

    def __init__(self, sc, dataset_path):
        """
        初始化
        """

        logger.info("Starting up the Recommendation Engine: ")

        self.sc = sc

        # 加载评分数据备用
        logger.info("Loading Ratings data...")
        ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
        ratings_raw_RDD = self.sc.textFile(ratings_file_path)
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line!=ratings_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
        # 加载电影数据备用
        logger.info("Loading Movies data...")
        movies_file_path = os.path.join(dataset_path, 'movies.csv')
        movies_raw_RDD = self.sc.textFile(movies_file_path)
        movies_raw_data_header = movies_raw_RDD.take(1)[0]
        self.movies_RDD = movies_raw_RDD.filter(lambda line: line!=movies_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()
        self.movies_titles_RDD = self.movies_RDD.map(lambda x: (int(x[0]),x[1])).cache()
        # 预计电影评分数
        self.__count_and_average_ratings()

        # 训练模型
        self.rank = 8
        self.seed = 5
        self.iterations = 10
        self.regularization_parameter = 0.1
        self.__train_model() 
