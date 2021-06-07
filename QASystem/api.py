from flask import Flask, request, render_template
import json
from question_classifier import *
from question_parser import *
from answer_search import *
from flask_cors import CORS, cross_origin

server = Flask(__name__)
CORS(server, resources=r'/*')

@server.route('/index',methods=['get'])
def index():
    res = {}
    classifier = QuestionClassifier()
    parser = QuestionPaser()
    searcher = AnswerSearcher()
    answer = '您好，我是小珞，希望可以帮您解决一些心理方面的问题。如果没有得到满意答案，欢迎反馈意见，祝您身体健康，天天开心！'

    if request.args is None:
        res['code'] = '5004'
        res['info'] = '请求参数为空'
        return json.dumps(res, ensure_ascii=False)

    param = request.args.to_dict()
    sent = param.get('sent')

    res_classify = classifier.classify(sent)
    if not res_classify:
        res['answer'] = answer
        return json.dumps(res, ensure_ascii = False)
        # return render_template('home.html')
    res_sql = parser.parser_main(res_classify)
    final_answers = searcher.search_main(res_sql)
    if not final_answers:
        return json.dumps(res, ensure_ascii = False)
    else:
        str = '\n'.join(final_answers)
        res['answer'] = str
        return json.dumps(res, ensure_ascii = False)


if __name__ == '__main__':
    server.config['JSON_AS_ASCII'] = False
    server.run(port=5001,debug=True)