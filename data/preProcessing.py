import json
import efaqa_corpus_zh

def get_corpus():
    l = list(efaqa_corpus_zh.load())
    print("size: %s" % len(l))
    print(l[2]['title'])


get_corpus()