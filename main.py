import os
from abc import abstractmethod

import pandas as pd
import re
import openpyxl

pd.set_option('display.max_columns', None)


class TropicanaDataPipeLine:
    def __init__(self, input_path: str, output_path: str, batch_id: str) -> None:
        self.input_path = input_path
        self.output_path = output_path
        self.batch_id = batch_id

    def extract(self) -> dict:
        with open(self.input_path, mode="r") as file:
            data = file.readlines()

        return data

    def describe_output(self, raw_data):
        pass
        # print(raw_data.shape)
        # print(raw_data.columns)

    def transform(self, raw_data):
        df = pd.DataFrame(raw_data)
        df.drop([4, 5, 6, 7, 8, 9, 10, 11, 22, 20, 12, 16, 14, 18], inplace=True, axis=1)
        df.rename(columns={0: "Class", 1: "Type",
                           2: "AccountNumber",
                           3: "AccountDescription",
                           13: "TranDate",
                           15: "TransactionDescription",
                           17: "Source",
                           19: "JE#",
                           21: "Amount",
                           23: "Cumulative"

                           }, inplace=True)
        df['TranDate'] = pd.to_datetime(df['TranDate'])
        df['BatchId'] = self.batch_id
        return df

    def clean(self, raw_data):
        class_ = ""
        type_ = ""
        acct_num_ = ""
        new_list = []
        account = ''
        account_des = ''
        for idx, row in enumerate(raw_data):
            if len(row) > 1 and idx > 3 and not row.__contains__("________________"):
                row_data_list = row.split('"')
                # print(idx, row_data_list)
                if len(row_data_list) == 3:
                    class_ = row_data_list[1]  # Get class
                if len(row_data_list) == 5:
                    type_ = row_data_list[3]  # get type Revenue or Expense
                if len(row_data_list) == 9:
                    acct_num_ = row_data_list[5:8]  # Get account number and description
                    acct_num_.remove(',')
                    account, account_des = acct_num_
                if len(row_data_list) == 21:  # Detail lines, this would be easier in pandas using the ffill function
                    ## add to the existing list the columns that were not inline
                    row_data_list.insert(0, class_)
                    row_data_list.insert(1, type_)
                    row_data_list.insert(2, account)
                    row_data_list.insert(3, account_des)
                    if len(row_data_list[:24]) != 0:
                        new_list.append(row_data_list[:24])

        return new_list

    def load_data(self, data):
        file = os.path.join(self.output_path, 'output_file.xlsx')
        data.to_excel(file)  # outfile to excel


# if '__name__' == '__main__':
file = TropicanaDataPipeLine(f"input/2302 Program report_27032023.csv", "output", batch_id=2302)
data = file.extract()  # get data from source
cleaned = file.clean(raw_data=data)  # clean file
transformed = file.transform(cleaned)  # add transformations
file.describe_output(transformed)  # optional, describe the output
file.load_data(transformed)  # output to an excel file
