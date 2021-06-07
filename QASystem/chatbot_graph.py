# coding: utf-8
from question_classifier import *
from question_parser import *
from answer_search import *

class ChatBotGraph:
    def __init__(self):
        self.classifier = QuestionClassifier()
        self.parser = QuestionPaser()
        self.searcher = AnswerSearcher()

    def chat_main(self, sent):
        answer = '您好，我是小珞，希望可以帮您解决一些心理方面的问题。如果没有得到满意答案，欢迎反馈意见，祝您身体健康，天天开心！'
        res_classify = self.classifier.classify(sent)
        if not res_classify:
            return answer
        res_sql = self.parser.parser_main(res_classify)
        final_answers = self.searcher.search_main(res_sql)
        if not final_answers:
            return answer
        else:
            return '\n'.join(final_answers)

if __name__ == '__main__':
    handler = ChatBotGraph()
    # while 1:
    for i in range(4):
        question = input('咨询问题:')
        answer = handler.chat_main(question)
        print('小珞:', answer)

