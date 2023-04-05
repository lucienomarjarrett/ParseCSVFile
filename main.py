import os
import pandas as pd
import data_connection.db_connectors as db
pd.set_option('display.max_columns', None)



class TropicanaETL:
    '''
    class for extract, transform and load
    '''
    def __init__(self, input_path: str, output_path: str, batch_id: str) -> None:
        self.input_path = input_path #source location for csv
        self.output_path = output_path #destination path for xl file
        self.batch_id = batch_id #assign a batchid


    def extract(self):
        with open(self.input_path, mode="r") as file:
            data = file.readlines()

        return data

    def transform(self, raw_data):
        df = pd.DataFrame(raw_data)
        df.drop([4, 5, 6, 7, 8, 9, 10, 11, 22, 20, 12, 16, 14, 18], inplace=True, axis=1)
        df.rename(columns={0: "Class",
                           1: "Type",
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
        df['Amount'] = df['Amount'].str.replace(",",'').astype(float)
        df['Cumulative'] = df['Cumulative'].str.replace(",", '').astype(float)

        return df

    def clean(self, raw_data):
        class_ = ""
        type_ = ""
        acct_num_ = ""
        new_list = []
        account = ''
        account_des = ''
        try:
            for idx, row in enumerate(raw_data):
                if len(row) > 1 and idx > 3 and not row.__contains__("________________"):
                    row_data_list = row.split('"')
                    # print(idx, row_data_list)
                    if len(row_data_list) == 3:
                        class_ = row_data_list[1]  # Get class eg Administration
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
        except Exception as e:
            print(f"Error {e}")
        return new_list

    def load_data_to_db(self, data):
        conn = db.connection_str()
        cursor = conn.cursor()
        try:
            for idx, row in data.iterrows():
                db.insert_row(cursor,
                              row['Class'], \
                              row['Type'], \
                              row['AccountNumber'] \
                              ,row['AccountDescription'], \
                              row['TranDate'], \
                              row['TransactionDescription'], \
                              row['Source'], \
                              row['JE#'], \
                              row['Amount'],
                              row['Cumulative'],
                              self.batch_id)

                # print(row)
            conn.commit()
            cursor.close()
        except Exception as e:
            print(f"Error {e}")

    def load_data_to_xl(self, data):
        file = os.path.join(self.output_path, 'output_file.xlsx')
        data.to_excel(file)  # outfile to excel

if __name__ == '__main__':
    file = TropicanaETL(f"input/2302 Program report_03042023.csv", "output", batch_id=2302) #batch_id needs \
    # to be added
    data = file.extract()  # get data from source
    cleaned = file.clean(raw_data=data)  # clean file
    transformed = file.transform(cleaned)  # add transformations
    file.load_data_to_xl(transformed)  # output to an excel file
    file.load_data_to_db(transformed) # output to database, connections string can be modified.
