"""
定义actions动作
@author ZhangZhe<3330843748@qq.com>
@date 2023.08.26
@version 1.0.0
"""

# This is a simple example for a custom action which utters "Hello World!"
from py2neo import Graph, NodeMatcher
import json
from typing import Any, Text, Dict, List

from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher


class ActionRecommendMovie(Action):

    def name(self) -> Text:
        return "action_recommend_movie"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        movie_type = next(tracker.get_latest_entity_values("movie_type"), None)
        director = next(tracker.get_latest_entity_values("director"), None)
        actor = next(tracker.get_latest_entity_values("actor"), None)
        time = next(tracker.get_latest_entity_values("time"), None)
        single_time = next(tracker.get_latest_entity_values("single_time"), None)
        country = next(tracker.get_latest_entity_values("country"), None)
        ## 连接数据库
        graph = Graph("http://localhost:7474/browser/", auth=('xiaowuchanglu', 'zz080222zz'), name='movieGraph')
        cypher = 'MATCH(n:movie) where '
        if movie_type is not None:
            cypher += 'n.type = ' + '\'' + movie_type + '\'' + ' and '
        if director is not None:
            cypher += 'n.director =~ ' + '\'' + director + '.*\'' + ' and '
        if actor is not None:
            cypher += 'n.actor =~ ' + '\'' + actor + '.*\'' + ' and '
        if country is not None:
            cypher += 'n.country = ' + '\'' + country + '\'' + ' and '
        if single_time is not None:
            cypher += 'n.time = ' + '\'' + single_time + '\'' + ' '
        elif time is not None:
            begin_time = '\'' + time.split(',')[0] + '\''
            end_time = '\'' + time.split(',')[-1] + '\''
            # dispatcher.utter_message('tile: ' + time)
            # dispatcher.utter_message('begin_time' + begin_time)
            # dispatcher.utter_message('end_time' + end_time)
            cypher += 'n.time >= ' + begin_time + ' and n.time <= ' + end_time + ' '
        if cypher.endswith(' and '):
            cypher = cypher[0:-4]

        if cypher != 'MATCH(n:movie) where ':
            cypher += 'return n.name'
            # dispatcher.utter_message(cypher)
            movie_names = graph.run(cypher).data()
            if len(movie_names) != 0:
                movie_list_str = '你要找的电影有：'
                for movie_name in movie_names:
                    movie_list_str += movie_name['n.name'] + '、'
                movie_list_str = movie_list_str[0:-1]
                dispatcher.utter_message(movie_list_str)
            else:
                dispatcher.utter_message('对不起，我暂时不知道您要找的电影')

        else:
            dispatcher.utter_message('对不起，您没有输入类型等详细信息，我暂时不知道您要找的电影')

        return []


class ActionDetailOfMovie(Action):

    def name(self) -> Text:
        return "action_detail_of_movie"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        movie = next(tracker.get_latest_entity_values("movie"), None)
        if movie is None:
            dispatcher.utter_message("对不起，我暂时不理解您说的是哪部电影")
        else:
            actor_entity_name = next(tracker.get_latest_entity_values("actor_entity_name"), None)
            director_entity_name = next(tracker.get_latest_entity_values("director_entity_name"), None)
            movie_type_entity_name = next(tracker.get_latest_entity_values("movie_type_entity_name"), None)
            single_time_entity_name = next(tracker.get_latest_entity_values("single_time_entity_name"), None)
            rate_entity_name = next(tracker.get_latest_entity_values("rate_entity_name"), None)
            num_entity_name = next(tracker.get_latest_entity_values('num_entity_name'), None)
            country_entity_name = next(tracker.get_latest_entity_values('country_entity_name'), None)
            ## 连接数据库
            graph = Graph("http://localhost:7474/browser/", auth=('xiaowuchanglu', 'zz080222zz'), name='movieGraph')
            cypher = 'MATCH(n:movie) where n.name = ' + '\'' + movie + '\' return n'
            data_result = graph.run(cypher).data()
            if len(data_result) == 0:
                dispatcher.utter_message('对不起，我暂时不知道您要找的电影')
            else:
                data_list = dict(data_result[0]['n'])
                if actor_entity_name is None and director_entity_name is None and movie_type_entity_name is None and single_time_entity_name is None and rate_entity_name is None and num_entity_name is None and country_entity_name is None:
                    message_str = movie + '是' + data_list['director'] + '导演, ' + data_list[
                        'actor'] + '主演, ' + '于' + \
                                  str(data_list['time']) + '在' + data_list['country'] + '上映的一部' + data_list[
                                      'type'] + '电影， ' + '这部电影在上映时获得了' + data_list[
                                      'num'] + '的票房' + ', 豆瓣评分' + str(
                        data_list['rate']) + '以下是这部电影的简介：\n' + \
                                  data_list['info']
                    dispatcher.utter_message(message_str)
                else:
                    message_str = movie
                    if director_entity_name is not None:
                        message_str += '是' + data_list['director'] + '导演的 '
                    if actor_entity_name is not None:
                        message_str += '是' + data_list['actor'] + '主演的 '
                    if movie_type_entity_name is not None:
                        message_str += '是一部' + data_list['type'] + '电影 '
                    if single_time_entity_name is not None:
                        message_str += '于' + str(data_list['time']) + '上映 '
                    if rate_entity_name is not None:
                        message_str += '豆瓣评分' + str(data_list['rate']) + ' '
                    if num_entity_name is not None:
                        message_str += '上映票房' + data_list['num'] + ' '
                    if country_entity_name is not None:
                        message_str += '在' + data_list['country'] + '上映 '
                    dispatcher.utter_message(message_str)

        return []


class ActionFallBack(Action):

    def name(self) -> Text:
        return "my_fallback_action"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        movie_type = next(tracker.get_latest_entity_values("movie_type"), None)
        director = next(tracker.get_latest_entity_values("director"), None)
        actor = next(tracker.get_latest_entity_values("actor"), None)
        time = next(tracker.get_latest_entity_values("time"), None)
        single_time = next(tracker.get_latest_entity_values("single_time"), None)
        country = next(tracker.get_latest_entity_values("country"), None)
        ## 连接数据库
        graph = Graph("http://localhost:7474/browser/", auth=('xiaowuchanglu', 'zz080222zz'), name='movieGraph')
        cypher = 'MATCH(n:movie) where '
        if movie_type is not None:
            cypher += 'n.type = ' + '\'' + movie_type + '\'' + ' and '
        if director is not None:
            cypher += 'n.director =~ ' + '\'' + director + '.*\'' + ' and '
        if actor is not None:
            cypher += 'n.actor =~ ' + '\'' + actor + '.*\'' + ' and '
        if country is not None:
            cypher += 'n.country = ' + '\'' + country + '\'' + ' and '
        if single_time is not None:
            cypher += 'n.time = ' + '\'' + single_time + '\'' + ' '
        elif time is not None:
            begin_time = '\'' + time.split(',')[0] + '\''
            end_time = '\'' + time.split(',')[-1] + '\''
            # dispatcher.utter_message('tile: ' + time)
            # dispatcher.utter_message('begin_time' + begin_time)
            # dispatcher.utter_message('end_time' + end_time)
            cypher += 'n.time >= ' + begin_time + ' and n.time <= ' + end_time + ' '
        if cypher.endswith(' and '):
            cypher = cypher[0:-4]

        if cypher != 'MATCH(n:movie) where ':
            cypher += 'return n.name'
            # dispatcher.utter_message(cypher)
            movie_names = graph.run(cypher).data()
            if len(movie_names) != 0:
                movie_list_str = '你要找的电影有：'
                for movie_name in movie_names:
                    movie_list_str += movie_name['n.name'] + '、'
                movie_list_str = movie_list_str[0:-1]
                dispatcher.utter_message(movie_list_str)
            else:
                dispatcher.utter_message('对不起，我暂时不知道您要找的电影')

        else:
            dispatcher.utter_message('对不起，您没有输入类型等详细信息，我暂时不知道您要找的电影')

        return []
