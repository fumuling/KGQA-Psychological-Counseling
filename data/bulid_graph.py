# coding: utf-8
import py2neo
import json
from tqdm import tqdm

class PsychologyGraph:
    def __init__(self):
        """
        定义连接neo4j数据库
        """
        self.connection = py2neo.Graph(
            host='127.0.0.1',
            http_port=7474,
            user='neo4j',
            password='ccLL.86916'
        )

        self.diseases = []  # 疾病
        self.alternate_name = []  # 疾病别称
        self.pathogenic_site = []  # 感染部位
        self.symptom = []  # 症状
        self.check = []  # 检查项目
        self.department = []  # 科室
        self.susceptible_crowd = []  # 易感人群

        self.disease_alternate_name = []  # 疾病与别名之间的关系
        self.disease_pathogenic_site = []  # 疾病与感染部位之间的关系
        self.disease_symptom = []  # 疾病-症状之间的关系
        self.disease_check = []  # 疾病-检查项目之间的关系
        self.disease_department = []  # 疾病-科室之间的关系
        self.disease_complication = []  # 疾病与疾病之间的并发关系
        self.disease_confusable = []  # 疾病之间的易混淆关系
        self.disease_crowd = []  # 疾病与易感人群的关系

        self.expert_remind = []  # 疾病的属性-专家提醒
        self.infectivity = []  # 疾病的属性-传染性
        self.heredity = []  # 疾病的属性-遗传性

    def read_data(self):
        """
        读取json，抽取出三元组
        """
        print('开始处理数据')
        with open('data.json', 'r', encoding='utf-8') as f:
            json_dic = json.load(f)
            for disease, info in json_dic.items():
                self.diseases.append(disease)
                disease_attribute = {}
                # 读取别名
                if info['alternate_name']:
                    alternate_name_list = info['alternate_name'].split('、')
                    self.alternate_name.extend(alternate_name_list)
                    for alternate_name in alternate_name_list:
                        self.disease_alternate_name.append([disease, 'alternate_name', alternate_name])
                # 读取感染部位
                if info['pathogenic_site']:
                    self.pathogenic_site.append(info['pathogenic_site'])
                    self.disease_pathogenic_site.append([disease, 'pathogenic_site', info['pathogenic_site']])
                # 读取科室
                if info['department']:
                    department_list = info['department'].split(',')
                    self.department += department_list
                    for department in department_list:
                        self.disease_department.append([disease, 'department', department])
                # 读取症状
                if info['symptom']:
                    symptom_list = info['symptom'].split('、')
                    self.symptom += symptom_list
                    for symptom in symptom_list:
                        self.disease_symptom.append([disease, 'has_symptom', symptom])
                # 读取检查项目
                if info['check']:
                    check_list = info['check'].split('、')
                    self.check += check_list
                    for check in check_list:
                        self.disease_check.append([disease, 'check', check])
                # 读取并发症
                if info['complication']:
                    complication_list = info['complication'].split('、')
                    self.diseases += complication_list
                    for complication in complication_list:
                        self.disease_complication.append([disease, 'accompany_with', complication])
                # 读取感染性
                if info['infectivity']:
                    if info['infectivity'][2] == '不':
                        self.infectivity.append([disease, False])
                    else:
                        self.infectivity.append([disease, True])
                # 读取遗传性
                if info['heredity']:
                    if info['heredity'][0] == '不':
                        self.heredity.append([disease, False])
                    else:
                        self.heredity.append([disease, True])
                # 读取混淆病症
                if info['confusable_disease']:
                    confusable_disease_list = info['confusable_disease'].split('、')
                    self.diseases += confusable_disease_list
                    for confusable_disease in confusable_disease_list:
                        self.disease_confusable.append([disease, 'accompany_with', confusable_disease])
                # 读取易感人群
                if info['susceptible_crowd']:
                    self.susceptible_crowd.append(info['susceptible_crowd'])
                    self.disease_crowd.append([disease, 'susceptible_crowd', info['susceptible_crowd']])
                # 读取专家建议
                if info['expert_remind']:
                    self.expert_remind.append([disease, info['expert_remind'].strip()])
        print('处理完成')

    def build_nodes(self, nodes_list, node_type):
        """
        建立结点
        :param node_type: 结点的标签
        :param nodes_list: 结点list
        :return: None
        """
        for node in tqdm(set(nodes_list)):
            cql = """
            MERGE (n:{node_type}{{name:'{node_name}'}})
            """.format(node_type=node_type, node_name=node)
            try:
                self.connection.run(cql)
            except Exception as e:
                print(e)
                print(cql)

    def build_relationship(self, triples_list, head_type, tail_type):
        """
        结点建立之后，建立结点之间的关系
        :param triples_list: list形式的三元组
        :param head_type: 首结点的标签
        :param tail_type: 尾结点的标签
        :return: None
        """
        for head, rels, tail in tqdm(triples_list):
            cql = """
            MATCH (p:{head_type}), (q:{tail_type})
            WHERE p.name='{head_name}' AND q.name='{tail_name}'
            MERGE (p)-[r:{relationship_name}]->(q)
            """.format(head_type=head_type, tail_type=tail_type, head_name=head, tail_name=tail, relationship_name=rels)
            try:
                self.connection.run(cql)
            except Exception as e:
                print(e)
                print(cql)

    def add_attr(self, tuples_list, node_type, attr):
        """
        结点建立之后，建立疾病结点的属性
        :param tuples_list: 实体属性之间的二元组
        :param node_type: 结点的标签
        :param attr: 属性的type
        :return: None
        """
        for node, attr_value in tqdm(tuples_list):
            cql = """
            MATCH (p:{node_type})
            WHERE p.name='{name}'
            SET p.{attr} = '{attr_value}'
            """.format(node_type=node_type, name=node, attr=attr, attr_value=attr_value)
            try:
                self.connection.run(cql)
            except Exception as e:
                print(e)
                print(cql)

    def run(self):
        # 读取数据，存入实例中
        self.read_data()

        # 创建实体结点
        self.build_nodes(self.diseases, 'disease')
        self.build_nodes(self.alternate_name, 'alternate_name')
        self.build_nodes(self.pathogenic_site, 'pathogenic_site')
        self.build_nodes(self.symptom, 'symptom')
        self.build_nodes(self.check, 'check')
        self.build_nodes(self.department, 'department')
        self.build_nodes(self.susceptible_crowd, 'susceptible_crowd')

        # 创建实体之间的关系
        self.build_relationship(self.disease_pathogenic_site, 'disease', 'pathogenic_site')
        self.build_relationship(self.disease_alternate_name, 'disease', 'alternate_name')
        self.build_relationship(self.disease_symptom, 'disease', 'symptom')
        self.build_relationship(self.disease_check, 'disease', 'check')
        self.build_relationship(self.disease_department, 'disease', 'department')
        self.build_relationship(self.disease_complication, 'disease', 'disease')
        self.build_relationship(self.disease_confusable, 'disease', 'disease')
        self.build_relationship(self.disease_crowd, 'disease', 'susceptible_crowd')

        # 设置实体的属性
        self.add_attr(self.expert_remind, 'disease', 'expert_remind')
        self.add_attr(self.infectivity, 'disease', 'infectivity')
        self.add_attr(self.heredity, 'disease', 'heredity')


if __name__ == '__main__':
    graph = PsychologyGraph()
    graph.run()
