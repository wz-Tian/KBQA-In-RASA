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
        dispatcher.utter_message('ok')

        return []
