"""
根据知识图谱获取需要的实体作为词库
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


#
#
class ActionRecommendMovie(Action):

    def name(self) -> Text:
        return "action_recommend_movie"

    # @staticmethod
    # def required_slots(tracker):
    #     return ["movie_type", "director", "actor", "time", "single_time", "country"]
    #
    # def slot_mapping(self):
    #     return {
    #         "movie_type": self.from_entity(entity="movie_type"),
    #         "director": self.from_entity(entity="director"),
    #         "actor": self.from_entity(entity="actor"),
    #         "time": self.from_entity(entity="time"),
    #         "single_time": self.from_entity(entity="single_time"),
    #         "country": self.from_entity(entity="country")
    #
    #     }

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        movie_type = next(tracker.get_latest_entity_values("movie_type"), None)
        director = next(tracker.get_latest_entity_values("director"), None)
        actor = next(tracker.get_latest_entity_values("actor"), None)
        time = next(tracker.get_latest_entity_values("time"), None)
        single_time = next(tracker.get_latest_entity_values("single_time"), None)
        country = next(tracker.get_latest_entity_values("country"), None)
        # if movie_type is None:
        #     dispatcher.utter_message('movie_type: ' + 'None' + '\n')
        # else:
        #     dispatcher.utter_message('movie_type: ' + movie_type + '\n')
        #
        # if director is None:
        #     dispatcher.utter_message('director: ' + 'None' + '\n')
        # else:
        #     dispatcher.utter_message('director: ' + director + '\n')
        #
        # if actor is None:
        #     dispatcher.utter_message('actor: ' + 'None' + '\n')
        # else:
        #     dispatcher.utter_message('actor: ' + actor + '\n')
        #
        # if time is None:
        #     dispatcher.utter_message('time: ' + 'None' + '\n')
        # else:
        #     time_list = time.split(',')
        #     dispatcher.utter_message('time: ' + time_list + '\n')
        #
        # if single_time is None:
        #     dispatcher.utter_message('single_time: ' + 'None' + '\n')
        # else:
        #     dispatcher.utter_message('single_time: ' + single_time + '\n')
        #
        # if country is None:
        #     dispatcher.utter_message('country: ' + 'None' + '\n')
        # else:
        #     dispatcher.utter_message('country: ' + country + '\n')
        ## 连接数据库
        graph = Graph("http://localhost:7474/browser/", auth=('xiaowuchanglu', 'zz080222zz'), name='movieGraph')
        node_matcher = NodeMatcher(graph)

        # movie_type：1  director：1  actor：1 time：0  single_time：1   country：1
        # if movie_type is not None and director is not None and actor is not None and single_time is not None and country is not None:
        #     cypher = 'MATCH(n:movie{type:' + movie_type + ', director:' + director + ', actor:' + actor + ', time:' + str(
        #         single_time) + ', country:' + country + '}) return n.name'
        # # movie_type：1  director：1  actor：1 time：1  single_time：0   country：1
        # begin_time = time.split(',')[0]
        # end_time = time.split(',')[-1]
        # if movie_type is not None and director is not None and actor is not None and single_time is not None and country is not None:
        #     cypher = 'MATCH(n:movie{type:' + movie_type + ', director:' + director + ', actor:' + actor + ', country:' + country + '})' + 'where n.time >= ' + begin_time + 'and n.time <= ' + end_time +
        #     + ' return n.name'
        # movie_type：1  director：1  actor：1 time：0  single_time：1   country：0
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
            dispatcher.utter_message('tile: ' + time)
            dispatcher.utter_message('begin_time' + begin_time)
            dispatcher.utter_message('end_time' + end_time)
            cypher += 'n.time >= ' + begin_time + ' and n.time <= ' + end_time + ' '
        if cypher.endswith(' and '):
            cypher = cypher[0:-4]

        if cypher != 'MATCH(n:movie) where ':
            cypher += 'return n.name'
            dispatcher.utter_message(cypher)
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
