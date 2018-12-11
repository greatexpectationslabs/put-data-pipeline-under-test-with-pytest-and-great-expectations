import pandas as pd
import random

class SuperPipeline(object):

    def __init__(self):
        self.state = "inited"

    def process_file(self, file_path, out_file_path):
        df1 = pd.read_csv(file_path, sep=",", dtype=object)
        df1 = df1.drop(columns=['RecordID'])
        df1['Degree'] = df1.apply(lambda x: 'N.P.' if random.random() > 0.7 else x['Degree'], axis=1)

        def break_dept(dept):
            if random.random() > 0.2:
                return 'Surgery'
            else:
                return dept

        df1.to_csv(out_file_path, index=False)
