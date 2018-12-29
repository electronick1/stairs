import string
import json


from stairs.core.flow import Flow, step
from threading import Thread


class NameExtractionOneWayFlow(Flow):

    def __init__(self, use_lower=False):
        self.use_lower = use_lower

    def __call__(self, sentence):
        print("Im in NameExtractionOneWayFlow ")
        r = self.start_from(self.clean,
                            sentence=sentence)
        print(r)
        return r.result_name

    @step(None)
    def result_name(self, words):
        print("get name")
        result_names = []
        for name in ['Oleg']:
            if name in words:
                if self.use_lower:
                    name = name.lower()
                result_names.append(name)

        return dict(names=result_names)

    @step(result_name)
    def split_by_words(self, sentence):
        print("Split by words")
        return dict(words=sentence.split())

    @step(split_by_words)
    def remove_punctuation(self, sentence):
        print("remove_punctuation")
        for p in string.punctuation:
            sentence = sentence.replace(p, ' ')

        return dict(sentence=sentence)

    @step(remove_punctuation)
    def clean(self, sentence):
        print("clean")
        sentence = sentence.strip()
        return dict(sentence=sentence)


class NameExtractionFlowMultiple(NameExtractionOneWayFlow):

    def __call__(self, sentence):
        print("Im in NameExtractionFlowMultiple ")
        r = self.start_from(self.clean,
                            sentence=sentence)
        print(r)
        return dict(
            names=r.result_name['names'],
            sentence=r.clean['sentence']
        )

    @step(NameExtractionOneWayFlow.remove_punctuation, save_result=True)
    def clean(self, sentence):
        print("clean")
        sentence = sentence.strip()
        return dict(sentence=sentence)


class NameExtractionWorkersOneWayFlow(NameExtractionOneWayFlow):

    def __reconnect__(self):
        self.result_name.set_next(self.save_worker_result)

    def __call__(self, sentence):
        result = self.start_from(self.clean,
                                 sentence=sentence)

        return dict()

    @step(None)
    def save_worker_result(self, **kwargs):
        self.redis.set("test", json.dumps(kwargs))

