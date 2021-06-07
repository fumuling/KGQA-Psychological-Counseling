# coding: utf-8

class QuestionPaser:
    '''
    构建实体节点
    '''
    def build_entitydict(self, args):
        entity_dict = {}
        for arg, types in args.items():
            for type in types:
                if type not in entity_dict:
                    entity_dict[type] = [arg]
                else:
                    entity_dict[type].append(arg)

        return entity_dict

    def parser_main(self, res_classify):
        args = res_classify['args']
        entity_dict = self.build_entitydict(args)
        question_types = res_classify['question_types']
        sqls = []
        for question_type in question_types:
            sql_ = {}
            sql_['question_type'] = question_type
            sql = []
            if question_type == 'disease_symptom':
                sql = self.sql_transfer(question_type, entity_dict.get('disease'))
            elif question_type == 'symptom_disease':
                sql = self.sql_transfer(question_type, entity_dict.get('symptom'))
            elif question_type == 'disease_accompany':
                sql = self.sql_transfer(question_type, entity_dict.get('disease'))
            elif question_type == 'disease_check':
                sql = self.sql_transfer(question_type, entity_dict.get('disease'))
            elif question_type == 'check_disease':
                sql = self.sql_transfer(question_type, entity_dict.get('check'))
            elif question_type == 'disease_prevent':
                sql = self.sql_transfer(question_type, entity_dict.get('disease'))
            elif question_type == 'disease_cureway':
                sql = self.sql_transfer(question_type, entity_dict.get('disease'))
            elif question_type == 'disease_easyget':
                sql = self.sql_transfer(question_type, entity_dict.get('disease'))
            if sql:
                sql_['sql'] = sql
                sqls.append(sql_)

        return sqls


    def sql_transfer(self, question_type, entities):
        '''
        针对不同的问题，分开进行处理
        '''
        if not entities:
            return []
        # 查询语句
        sql = []
        # 查询疾病的专家建议
        if question_type == 'disease_prevent':
            sql = ["MATCH (m:disease) where m.name = '{0}' return m.name, m.expert_remind".format(i) for i in entities]
        # 查询疾病的易发人群
        elif question_type == 'disease_easyget':
            sql = ["MATCH (m:disease)-[r:susceptible_crowd]->(n:susceptible_crowd) where m.name = '{0}' return m.name, r.name, n.name".format(i) for i in entities]
        # 查询疾病有哪些症状
        elif question_type == 'disease_symptom':
            sql = ["MATCH (m:disease)-[r:has_symptom]->(n:symptom) where m.name = '{0}' return m.name, r.name, n.name".format(i) for i in entities]
        # 查询症状会导致哪些疾病
        elif question_type == 'symptom_disease':
            sql = ["MATCH (m:disease)-[r:has_symptom]->(n:symptom) where n.name = '{0}' return m.name, r.name, n.name".format(i) for i in entities]
        # 查询疾病的并发症
        elif question_type == 'disease_accompany':
            sql1 = ["MATCH (m:disease)-[r:accompany_with]->(n:disease) where m.name = '{0}' return m.name, r.name, n.name".format(i) for i in entities]
            sql2 = ["MATCH (m:disease)-[r:accompany_with]->(n:disease) where n.name = '{0}' return m.name, r.name, n.name".format(i) for i in entities]
            sql = sql1 + sql2
        # 查询疾病应该进行的检查
        elif question_type == 'disease_check':
            sql = ["MATCH (m:disease)-[r:check]->(n:check) where m.name = '{0}' return m.name, r.name, n.name".format(i) for i in entities]
        # 已知检查查询疾病
        elif question_type == 'check_disease':
            sql = ["MATCH (m:disease)-[r:check]->(n:check) where n.name = '{0}' return m.name, r.name, n.name".format(i) for i in entities]

        return sql

if __name__ == '__main__':
    handler = QuestionPaser()
