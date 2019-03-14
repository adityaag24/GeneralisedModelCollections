class InvoiceLevelFeatures():

    def __init__(self,data,history_date,train_date,customer_identifier):
        self.data = data
        self.history_date = history_date
        self.train_date = train_date
        self.customer_identifier = customer_identifier

    def getTrainTestData(self):
        import pandas as pd
        self.data['mega_date'] = pd.to_datetime(self.data['mega_date'])
        self.train_test = self.data[self.data['mega_date'] >= self.history_date]

    def createComboId(self):
        self.train_test['combo_id'] = self.train_test[self.customer_identifier].astype(str) + self.train_test['mega_date_num'].astype(str)
        self.train_test['combo_id'] = self.train_test['combo_id'].astype(float)

    def getUniqueCustIds(self):
        return self.data[self.customer_identifier].unique().tolist()

    def past_invoice_count(self):
        import pandas as pd
        unique_cust_id = self.getUniqueCustIds()
        combo_num = []
        count_list = []
        for cust in unique_cust_id:
            temp_old = pd.DataFrame()
            temp_old = self.data[self.data[self.customer_identifier] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test[self.customer_identifier] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                current_date = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                count = 0
                count = temp_old.loc[(temp_old.due_date_num < current_date) & (temp_old.clearing_date_num > current_date)].shape[0]
                count_list.append(count)
        invoice_zip1 = zip(combo_num, count_list)
        invoice_dict1 = dict(invoice_zip1)
        self.train_test['past_invoice_count'] = self.train_test['combo_id'].map(invoice_dict1)

    def past_invoice_count_baseline(self):
        import pandas as pd
        unique_cust_id = self.getUniqueCustIds()
        combo_num = []
        count_list = []
        for cust in unique_cust_id:
            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test[self.customer_identifier] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            temp_old = pd.DataFrame()
            temp_old = self.data[self.data[self.customer_identifier] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                current_date = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                count = temp_old.loc[
                    (temp_old.mega_date_num < current_date) & (temp_old.clearing_date_num > current_date)].shape[0]
                count_list.append(count)
        invoice_zip = zip(combo_num, count_list)
        invoice_dict = dict(invoice_zip)
        self.train_test['past_invoice_count_baseline'] = self.train_test['combo_id'].map(invoice_dict)

    def sum_base_amount_due(self):
        import pandas as pd
        unique_cust_id = self.getUniqueCustIds()
        combo_num = []
        sum_list = []
        for cust in unique_cust_id:
            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test[self.customer_identifier] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            temp_old = pd.DataFrame()
            temp_old = self.data[self.data[self.customer_identifier] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                current_date = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                total_object = temp_old.loc[
                    (temp_old.due_date_num < current_date) & (temp_old.clearing_date_num > current_date)]
                total_sum = total_object['invoice_amount_norm'].sum()
                sum_list.append(total_sum)
        payment_zip = zip(combo_num, sum_list)
        payment_dict = dict(payment_zip)
        self.train_test['sum_base_amount_due'] = self.train_test['combo_id'].map(payment_dict)

    def sum_base_amount_base(self):
        import pandas as pd
        unique_cust_id = self.getUniqueCustIds()
        combo_num = []
        payment_list = []
        for cust in unique_cust_id:
            temp_df = pd.DataFrame()
            temp_df = self.train_test.loc[self.train_test[self.customer_identifier] == cust, :]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            temp_old = pd.DataFrame()
            temp_old = self.data.loc[self.data[self.customer_identifier] == cust, :]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                current_date = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                total_object = temp_old.loc[
                    (temp_old.mega_date_num < current_date) & (temp_old.clearing_date_num > current_date)]
                total_sum = total_object['invoice_amount_norm'].sum()
                payment_list.append(total_sum)
        payment_zip = zip(combo_num, payment_list)
        payment_dict = dict(payment_zip)
        self.train_test['sum_base_amount_base'] = self.train_test['combo_id'].map(payment_dict)

    def past_n_invoice_count(self):
        import pandas as pd
        unique_cust_id = self.getUniqueCustIds()
        combo_num = []
        past_30_list = []
        past_60_list = []
        past_90_list = []
        for cust in unique_cust_id:
            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test[self.customer_identifier] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            temp_old = pd.DataFrame()
            temp_old = self.data[self.data[self.customer_identifier] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                curr_date = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                subset = temp_old.loc[
                    (temp_old['due_date_num'] < curr_date) & (temp_old['clearing_date_num'] > curr_date)]
                count1 = len(subset[(curr_date - subset['due_date_num']) <= 30])
                count2 = len(
                    subset[((curr_date - subset['due_date_num']) > 30) & ((curr_date - subset['due_date_num']) <= 60)])
                count3 = len(
                    subset[((curr_date - subset['due_date_num']) > 60) & ((curr_date - subset['due_date_num']) <= 90)])
                past_30_list.append(count1)
                past_60_list.append(count2)
                past_90_list.append(count3)
        bin1_zip = zip(combo_num, past_30_list)
        bin2_zip = zip(combo_num, past_60_list)
        bin3_zip = zip(combo_num, past_90_list)
        bin1_dict = dict(bin1_zip)
        bin2_dict = dict(bin2_zip)
        bin3_dict = dict(bin3_zip)
        self.train_test['past_30_invoice_count'] = self.train_test['combo_id'].map(bin1_dict)
        self.train_test['past_60_invoice_count'] = self.train_test['combo_id'].map(bin2_dict)
        self.train_test['past_90_invoice_count'] = self.train_test['combo_id'].map(bin3_dict)

    def last_clearing_date(self):
        import pandas as pd
        unique_cust_id = self.getUniqueCustIds()
        combo_num = []
        last_date_list = []
        for cust in unique_cust_id:
            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test[self.customer_identifier] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            temp_old = pd.DataFrame()
            temp_old = self.data[self.data[self.customer_identifier] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                create_date_curr = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                subset = temp_old[temp_old['clearing_date_num'] < create_date_curr]
                if len(subset) == 0:
                    last_date = objects['mega_date'].tolist()[0]
                else:
                    last_date = subset['clearing_date_norm'].max()
                    if last_date == '':
                        print('Null from else')
                last_date_list.append(last_date)
        last_date_zip = zip(combo_num, last_date_list)
        last_date_dict = dict(last_date_zip)
        self.train_test['last_clearing_date'] = self.train_test['combo_id'].map(last_date_dict)

    def past_total_avg_x(self):
        import pandas as pd
        import numpy as np
        unique_cust_id = self.getUniqueCustIds()
        combo_num = []
        past_total_avg_delay = []
        past_total_avg_delay_count = []
        for cust in unique_cust_id:
            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test[self.customer_identifier] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            temp_old = pd.DataFrame()
            temp_old = self.data[self.data[self.customer_identifier] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                current_date = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                start_date = current_date - 365
                subset = temp_old.loc[(
                            (temp_old.due_date_num < current_date) | (temp_old.clearing_date_num < current_date) & (
                                temp_old.mega_date_num > current_date))]
                count = len(subset)
                if count != 0:
                    subset['mega_delay'] = np.where(subset['clearing_date_num'] > current_date,
                                                    current_date - subset['due_date_num'], subset['delay'])
                    total_avg_delay = subset['mega_delay'].sum() / count
                else:
                    total_avg_delay = 0
                past_total_avg_delay.append(total_avg_delay)
                past_total_avg_delay_count.append(count)
        invoice_zip1 = zip(combo_num, past_total_avg_delay)
        invoice_zip2 = zip(combo_num, past_total_avg_delay_count)
        invoice_dict1 = dict(invoice_zip1)
        invoice_dict2 = dict(invoice_zip2)
        self.train_test['past_total_avg_delay'] = self.train_test['combo_id'].map(invoice_dict1)
        self.train_test['past_total_delay_count'] = self.train_test['combo_id'].map(invoice_dict2)

    def past_avg_delay_x(self):
        import pandas as pd
        import numpy as np
        unique_cust_id = self.getUniqueCustIds()
        combo_num = []
        past_avg_delay = []
        past_avg_delay_count = []
        for cust in unique_cust_id:
            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test['customer_number_norm'] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            temp_old = pd.DataFrame()
            temp_old = self.data[self.data['customer_number_norm'] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                current_date = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                start_date = current_date - 365
                subset = temp_old.loc[
                    ((temp_old.due_date_num < current_date) | (temp_old.clearing_date_num < current_date)) & (
                                temp_old.delay > 0) & (temp_old.mega_date_num > start_date)]
                count = len(subset)
                if (count == 0):
                    average_delay = 0
                else:
                    subset['mega_delay'] = np.where(subset['clearing_date_num'] > current_date,
                                                    current_date - subset['due_date_num'], subset['delay'])
                    average_delay = subset['mega_delay'].sum() / count
                past_avg_delay.append(average_delay)
                past_avg_delay_count.append(count)
        invoice_zip1 = zip(combo_num, past_avg_delay)
        invoice_zip2 = zip(combo_num, past_avg_delay_count)
        invoice_dict1 = dict(invoice_zip1)
        invoice_dict2 = dict(invoice_zip2)
        self.train_test['past_avg_delay'] = self.train_test['combo_id'].map(invoice_dict1)
        self.train_test['past_delay_count'] = self.train_test['combo_id'].map(invoice_dict2)

    def past_all_invoice_count(self):
        import pandas as pd
        unique_cust_id = self.getUniqueCustIds()
        past_invoice_count = []
        combo_num = []
        for cust in unique_cust_id:
            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test[self.customer_identifier] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            temp_old = pd.DataFrame()
            temp_old = self.data[self.data[self.customer_identifier] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                current_date = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                start_date = current_date - 365
                count = temp_old.loc[
                    (temp_old['mega_date_num'] < current_date) & (temp_old['mega_date_num'] > start_date)].shape[0]
                past_invoice_count.append(count)
        count_zip = zip(combo_num, past_invoice_count)
        count_dict = dict(count_zip)
        self.train_test['past_all_invoice_count'] = self.train_test['combo_id'].map(count_dict)

    def past_closed_invoice_count(self):
        import pandas as pd
        unique_cust_id = self.getUniqueCustIds()
        past_closed_invoice_count = []
        combo_num = []
        for cust in unique_cust_id:
            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test[self.customer_identifier] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            temp_old = pd.DataFrame()
            temp_old = self.data[self.data[self.customer_identifier] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                current_date = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                start_date = current_date - 365
                count = temp_old.loc[
                    (temp_old['mega_date_num'] < current_date) & (temp_old['clearing_date_num'] < current_date) & (
                                temp_old['mega_date_num'] > start_date)].shape[0]
                past_closed_invoice_count.append(count)
        count_zip1 = zip(combo_num, past_closed_invoice_count)
        count_dict1 = dict(count_zip1)
        self.train_test['past_closed_invoice_count'] = self.train_test['combo_id'].map(count_dict1)

    def past_closed_delayed_invoice_count(self):
        import pandas as pd
        unique_cust_id = self.getUniqueCustIds()
        past_closed_delayed_invoice_count = []
        combo_num = []
        for cust in unique_cust_id:
            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test[self.customer_identifier] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            temp_old = pd.DataFrame()
            temp_old = self.data[self.data[self.customer_identifier] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                current_date = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                start_date = current_date - 365
                count = temp_old.loc[
                    (temp_old['mega_date_num'] < current_date) & (temp_old['clearing_date_num'] < current_date) & (
                                temp_old['delay'] > 0) & (temp_old['mega_date_num'] > start_date)].shape[0]
                past_closed_delayed_invoice_count.append(count)
        count_zip11 = zip(combo_num, past_closed_delayed_invoice_count)
        count_dict11 = dict(count_zip11)
        self.train_test['past_closed_delayed_invoice_count'] = self.train_test['combo_id'].map(count_dict11)

    def past_closed_invoice_sum(self):
        import pandas as pd
        unique_cust_id = self.getUniqueCustIds()
        past_closed_invoice_sum = []
        combo_num = []
        for cust in unique_cust_id:
            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test[self.customer_identifier] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            temp_old = pd.DataFrame()
            temp_old = self.data[self.data[self.customer_identifier] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                current_date = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                start_date = current_date - 365
                subset = temp_old.loc[
                    (temp_old['mega_date_num'] < current_date) & (temp_old['clearing_date_num'] < current_date) & (
                                temp_old['mega_date_num'] > start_date)]
                past_closed_invoice_sum.append(subset['invoice_amount_norm'].sum())
        count_zip4 = zip(combo_num, past_closed_invoice_sum)
        count_dict4 = dict(count_zip4)
        self.train_test['past_closed_invoice_sum'] = self.train_test['combo_id'].map(count_dict4)

    def past_closed_delayed_invoice_sum(self):
        import pandas as pd
        unique_cust_id = self.getUniqueCustIds()
        past_closed_delayed_invoice_sum = []
        combo_num = []
        for cust in unique_cust_id:
            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test[self.customer_identifier] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            temp_old = pd.DataFrame()
            temp_old = self.data[self.data[self.customer_identifier] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                current_date = float(name)
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                start_date = current_date - 365
                subset = temp_old.loc[
                    (temp_old['mega_date_num'] < current_date) & (temp_old['clearing_date_num'] < current_date) & (
                                temp_old['delay'] > 0) & (temp_old['mega_date_num'] > start_date)]
                past_closed_delayed_invoice_sum.append(subset['invoice_amount_norm'].sum())
        count_zip3 = zip(combo_num, past_closed_delayed_invoice_sum)
        count_dict3 = dict(count_zip3)
        self.train_test['past_closed_delayed_invoice_sum'] = self.train_test['combo_id'].map(count_dict3)

    def outstanding_delay_as_of_today(self):
        import pandas as pd
        unique_cust_id = self.getUniqueCustIds()
        combo_num = []
        count_list = []
        for cust in unique_cust_id:
            temp_old = pd.DataFrame()
            temp_old = self.data[self.data[self.customer_identifier] == cust]
            temp_old = temp_old.reset_index()
            temp_old.drop('index', axis=1, inplace=True)

            temp_df = pd.DataFrame()
            temp_df = self.train_test[self.train_test[self.customer_identifier] == cust]
            temp_df = temp_df.reset_index()
            temp_df.drop('index', axis=1, inplace=True)

            data_grouped = temp_df.groupby(['mega_date_num'])
            for name, objects in data_grouped:
                current_date = float(name)
                avg_delay_as_of_today = 0
                combo_num.append(objects['combo_id'].unique().tolist()[0])
                subset = temp_old.loc[
                    (temp_old.due_date_num < current_date) & (temp_old.clearing_date_num > current_date)]
                if len(subset) != 0:
                    subset['mega_delay'] = current_date - subset['due_date_num']
                    avg_delay_as_of_today = subset['mega_delay'].sum() / len(subset)
                else:
                    avg_delay_as_of_today = 0
                count_list.append(avg_delay_as_of_today)
        invoice_zip12 = zip(combo_num, count_list)
        invoice_dict12 = dict(invoice_zip12)
        self.train_test['outstanding_delay_as_of_today'] = self.train_test['combo_id'].map(invoice_dict12)

    def Time_convert(stamp):
        import time
        from datetime import datetime
        l = int(time.mktime(stamp.timetuple()))
        return (l / 86400)

    def derived_features(self):
        import numpy as np
        import pandas as pd
        self.train_test['last_clearing_date_num'] = self.train_test['last_clearing_date'].apply(self.Time_convert)
        self.train_test['last_payment_date_difference'] = self.train_test['mega_date_num'] - self.train_test['last_clearing_date_num']
        self.train_test['gap_ratio'] = np.where(self.train_test['cust_avg_gap'] == 0, 0,self.train_test['last_payment_date_difference'] / self.train_test['cust_avg_gap'])
        self.train_test['sum_base_due'] = np.where(self.train_test['sum_base_amount_base'] == 0, 0,self.train_test['sum_base_amount_due'] / self.train_test['sum_base_amount_base'])
        self.train_test['past_invoice_due'] = np.where(self.train_test['past_invoice_count_baseline'] == 0, 0,self.train_test['past_invoice_count'] / self.train_test['past_invoice_count_baseline'])
        self.train_test['past_delay_payment_percent'] = np.where(self.train_test['past_total_delay_count'] == 0, 0, (self.train_test['past_delay_count'] / self.train_test['past_total_delay_count'] * 100))
        self.train_test['closed_delayed_sum_ratio'] = np.where(self.train_test['past_closed_invoice_sum'] == 0, 0,self.train_test['past_closed_delayed_invoice_sum'] / self.train_test['past_closed_invoice_sum'])
        self.train_test['closed_delayed_count_ratio'] = np.where(self.train_test['past_closed_invoice_count'] == 0, 0,self.train_test['past_closed_delayed_invoice_count'] /self.train_test['past_closed_invoice_count'])
        self.train_test['due_date_norm'] = pd.to_datetime(self.train_test['due_date_norm'])
        self.train_test['week_due'] = (self.train_test['due_date_norm']).dt.dayofweek
        self.train_test['month_due'] = (self.train_test['due_date_norm']).dt.month
        self.train_test['day_due'] = (self.train_test['due_date_norm']).dt.day

    def ILCalculations(self):
        self.getTrainTestData()
        self.createComboId()
        print('Calculating Past Invoice Count')
        self.past_invoice_count()
        print('Calculating Past Invoice Count Baseline')
        self.past_invoice_count_baseline()
        print('Calculating Sum Base Amount Base')
        self.sum_base_amount_base()
        print('Calculating Sum Base Amount Due')
        self.sum_base_amount_due()
        print('Calculating Past_n_Invoice_Count')
        self.past_n_invoice_count()
        print('Calculating Last Clearing Date')
        self.last_clearing_date()
        print('Calculating Past Total Average Count/Amount')
        self.past_total_avg_x()
        print('Calculating Past Average Delay Count/Amount')
        self.past_avg_delay_x()
        print('Calculating Past All Invoice Count')
        self.past_all_invoice_count()
        print('Calculating Past Delayed Invoice Count')
        self.past_closed_delayed_invoice_count()
        print('Calculating Past Closed Invoice Count')
        self.past_closed_invoice_count()
        print('Calculating Past Closed Invoice Sum')
        self.past_closed_invoice_sum()
        print('Calculating Past Closed Delayed Invoice Sum')
        self.past_closed_delayed_invoice_sum()
        print('Calculating Outstanding Delay As Of Today')
        self.outstanding_delay_as_of_today()
        print('Calculating Derived Features')
        self.derived_features()
