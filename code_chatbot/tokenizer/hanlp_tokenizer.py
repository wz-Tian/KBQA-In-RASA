"""
实现Hanlp分词器
@author wz_Tian
@date 2023.08.31
@version 1.0.0
"""

from __future__ import annotations
import glob
import logging
import os
import shutil
import typing
from typing import Any, Dict, List, Optional, Text

from rasa.nlu.components import Component
from rasa.nlu.tokenizers.tokenizer import Token, Tokenizer
from rasa.shared.nlu.training_data.message import Message

logger = logging.getLogger(__name__)


class HanlpTokenizer(Tokenizer):
    # provides=["tokens"]
    supported_language_list = ["zh"]

    def __init__(self, config: Dict[Text, Any]) -> None:

        super().__init__(config)


    @classmethod
    def required_packages(cls) -> List[Text]:
        return ["pyhanlp"]

    def tokenize(self, message: Message, attribute: Text) -> List[Token]:
        from pyhanlp import HanLP

        HanLP.Config.ShowTermNature = False
        text = message.get(attribute)

        tokenized = HanLP.segment(text)

        tokens = []
        start = 0

        for term in tokenized:
            tokens.append(Token(term.word, start))
            start += len(term.word)

        return self._apply_token_pattern(tokens)

    @classmethod
    def load(
            cls,
            config: Dict[Text, Any],
            model_dir: Text,
            model_metadata: Optional["Metadata"] = None,
            cached_component: Optional[Component] = None,
            **kwargs: Any,
    ) -> "HanlpTokenizer":

        # dictionary_path = config["dictionary_path"]

        return cls(config)

    @staticmethod
    def copy_files_dir_to_dir(input_dir: Text, output_dir: Text) -> None:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        target_file_list = glob.glob(f"{input_dir}/*")
        for target_file in target_file_list:
            shutil.copy2(target_file, output_dir)

    # def persist(self, file_name: Text, model_dir: Text) -> Optional[Dict[Text, Any]]:
    #
    #     # copy custom dictionaries to model dir, if any
    #     if self.dictionary_path is not None:
    #         target_dictionary_path = os.path.join(model_dir, file_name)
    #         self.copy_files_dir_to_dir(self.dictionary_path, target_dictionary_path)
    #
    #         return {"dictionary_path": file_name}
    #     else:
    #         return {"dictionary_path": None}