class CustomerLevelFeatures():

    def __init__(self,data,history_date,train_date,aggregator,limit):
        self.data = data
        self.history_date = history_date
        self.train_date = train_date
        self.aggregator = aggregator
        self.limit = limit

    def groupCustomers(self):
        grouped = self.data.groupby([self.aggregator])
        return grouped

    def importantCustomers(self,aggregator,limit):
        import inspect
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        params = [(i, values[i]) for i in args]
        print('Grouping Customers into Important and Non-Important with params : {}'.format(params))
        groupedCustomerNum = self.groupCustomers()
        important_customers = []
        for name, objects in groupedCustomerNum:
            if len(objects) >= limit:
                important_customers.append(name)
        return important_customers

    def delay_ratio(self):
        grouped = self.groupCustomers()
        important_customers = self.importantCustomers(self.aggregator,self.limit)
        imp_total = 0
        imp_delay = 0
        non_imp_total = 0
        non_imp_delay = 0
        list_data = []
        for name, objects in grouped:
            delay = len(objects[(objects['mega_date'] < self.history_date) & (objects['delay'] > 0)])
            total = len(objects[(objects['mega_date'] < self.history_date)])
            if name not in important_customers:
                objects['delay_ratio'] = -1
                non_imp_total = non_imp_total + total
                non_imp_delay = non_imp_delay + delay
            else:
                if total == 0:
                    objects['delay_ratio'] = 0
                else:
                    objects['delay_ratio'] = delay / total
                    imp_total = imp_total + total
                    imp_delay = imp_delay + delay
            list_data.append(objects['delay_ratio'])
        imp_avg = imp_delay / imp_total
        non_imp_avg = non_imp_delay / non_imp_total
        print('Total Average for Important Customers :', imp_avg)
        print('Total Average for Non-Important Customers :', non_imp_avg)
        list1 = []
        for df in list_data:
            list1.append(df.values)

        g = []
        for i in list1:
            for k in i:
                g.append(k)

        d = []
        for i in g:
            if (i == 0):
                i = imp_avg
            if (i == -1):
                i = non_imp_avg
            d.append(i)
        return d

    def delay_sum_ratio(self):
        grouped = self.groupCustomers()
        important_customers = self.importantCustomers(self.aggregator, self.limit)
        imp_total = 0
        imp_delay = 0
        non_imp_total = 0
        non_imp_delay = 0
        list_data = []
        for name, objects in grouped:
            delay_object = objects[(objects['mega_date'] < self.history_date) & (objects['delay'] > 0)]
            total_object = objects[objects['mega_date'] < self.history_date]
            total = total_object['invoice_amount_norm'].sum()
            delay = delay_object['invoice_amount_norm'].sum()
            if name not in important_customers:
                objects['delay_sum_ratio'] = -1
                non_imp_total = non_imp_total + total
                non_imp_delay = non_imp_delay + delay
            else:
                if total == 0:
                    objects['delay_sum_ratio'] = 0
                else:
                    objects['delay_sum_ratio'] = delay / total
                    imp_total = imp_total + total
                    imp_delay = imp_delay + delay
            list_data.append(objects['delay_sum_ratio'])
        imp_avg = imp_delay / imp_total
        non_imp_avg = non_imp_delay / non_imp_total
        print('Total Average for Important Customers :', imp_avg)
        print('Total Average for Non-Important Customers :', non_imp_avg)
        list1 = []
        for df in list_data:
            list1.append(df.values)

        g = []
        for i in list1:
            for k in i:
                g.append(k)

        d = []
        for i in g:
            if (i == 0):
                i = imp_avg
            if (i == -1):
                i = non_imp_avg
            d.append(i)
        return d

    def batch_size(self):
        grouped = self.groupCustomers()
        important_customers = self.importantCustomers(self.aggregator, self.limit)
        imp_total = 0
        imp_length = 0
        non_imp_total = 0
        non_imp_length = 0
        list_data = []
        for name, objects in grouped:
            cust_object = objects[objects['mega_date'] < self.history_date]
            unique_clearing_date = cust_object.clearing_date_norm.unique()
            imp_batch_count = 0
            non_imp_batch_count = 0
            if name not in important_customers:
                objects['batch_size'] = -1
                if (len(unique_clearing_date) == 0):
                    non_imp_batch_average = 0
                    non_imp_total = non_imp_total + non_imp_batch_average
                else:
                    for i in range(len(unique_clearing_date)):
                        non_imp_batch_count = non_imp_batch_count + len(
                            objects[objects['clearing_date_norm'] == unique_clearing_date[i]])
                    non_imp_batch_average = non_imp_batch_count / len(unique_clearing_date)
                    non_imp_total = non_imp_total + non_imp_batch_average
                    non_imp_length = non_imp_length + 1
            else:
                if len(unique_clearing_date) == 0:
                    objects['batch_size'] = 0
                else:
                    for i in range(len(unique_clearing_date)):
                        imp_batch_count = imp_batch_count + len(
                            objects[objects['clearing_date_norm'] == unique_clearing_date[i]])
                    imp_batch_avg = imp_batch_count / len(unique_clearing_date)
                    imp_total = imp_total + imp_batch_avg
                    imp_length = imp_length + 1
                    objects['batch_size'] = imp_batch_avg
            list_data.append(objects['batch_size'])
        average_block = imp_total / imp_length
        print("batch count for others", imp_length)
        print("total no of _customers in others: ", non_imp_length)
        non_imp_avg = non_imp_total / non_imp_length
        list11 = []
        for dk in list_data:
            list11.append(dk.values)

        g = []
        for i in list11:
            for k in i:
                g.append(k)

        d = []
        for i in g:
            if (i == 0):
                i = average_block
            if (i == -1):
                i = non_imp_avg
            d.append(i)
        return d

    def bat_size(self):
        grouped = self.groupCustomers()
        important_customers = self.importantCustomers(self.aggregator, self.limit)
        imp_counter = 0
        non_imp_counter = 0
        imp_total = 0
        non_imp_total = 0
        list_data = []
        for name, objects in grouped:
            cust_object = objects[objects['mega_date'] < self.history_date]
            unique_clearing_date = cust_object.clearing_date_norm.unique()
            imp_bat_count = 0
            non_imp_bat_count = 0
            if name not in important_customers:
                objects['bat_size'] = -1
                if (len(unique_clearing_date) != 0):
                    for i in range(0, len(unique_clearing_date)):
                        dataframe = objects[objects['clearing_date_norm'] == unique_clearing_date[i]]
                        non_imp_bat_count = non_imp_bat_count + len(dataframe.mega_date.unique())
                    non_imp_average = non_imp_bat_count / len(unique_clearing_date)
                    non_imp_total = non_imp_total + non_imp_average
                    non_imp_counter = non_imp_counter + 1
            else:
                if (len(unique_clearing_date) == 0):
                    objects['bat_size'] = 0
                else:
                    for i in range(0, len(unique_clearing_date)):
                        dataframe = objects[objects['clearing_date_norm'] == unique_clearing_date[i]]
                        imp_bat_count = imp_bat_count + len(dataframe.mega_date.unique())
                    imp_average = imp_bat_count / len(unique_clearing_date)
                    imp_total = imp_total + imp_average
                    objects['bat_size'] = imp_average
                    imp_counter = imp_counter + 1
            list_data.append(objects['bat_size'])
        print(imp_total / imp_counter)
        average_bat = imp_total / imp_counter
        non_imp_avg = non_imp_total / non_imp_counter
        list11 = []
        for df in list_data:
            list11.append(df.values)

        g = []
        for i in list11:
            for k in i:
                g.append(k)

        d = []
        for i in g:
            if (i == 0):
                i = average_bat
            if (i == -1):
                i = non_imp_avg
            d.append(i)
        return d

    def cust_avg_sum(self):
        grouped = self.groupCustomers()
        important_customers = self.importantCustomers(self.aggregator, self.limit)
        imp_total = 0
        non_imp_total = 0
        imp_len = 0
        non_imp_len = 0
        list_data = []
        for name, objects in grouped:
            cust_object = objects[objects['mega_date'] < self.history_date]
            total = cust_object['invoice_amount_norm'].sum()
            length = len(cust_object)
            if name not in important_customers:
                objects['cust_avg_sum'] = 0
                if (length != 0):
                    non_imp_total = non_imp_total + total
                    non_imp_len = non_imp_len + length
            else:
                if (length == 0):
                    objects['cust_avg_sum'] = 0
                else:
                    objects['cust_avg_sum'] = total / length
                    imp_total = imp_total + total
                    imp_len = imp_len + length
            list_data.append(objects['cust_avg_sum'])
        imp_avg = imp_total / imp_len
        non_imp_avg = non_imp_total / non_imp_len
        list3 = []
        for df in list_data:
            list3.append(df.values)

        g1 = []
        for i in list3:
            for k in i:
                g1.append(k)

        d1 = []
        for i in g1:
            if (i == 0):
                i = imp_avg
            if (i == -1):
                i = non_imp_avg
            d1.append(i)
        return d1

    def cust_total_avg_delay(self):
        grouped = self.groupCustomers()
        important_customers = self.importantCustomers(self.aggregator, self.limit)
        imp_total = 0
        imp_length = 0
        non_imp_total = 0
        non_imp_length = 0
        list_data = []
        for name, objects in grouped:
            cust_object = objects[objects['mega_date'] < self.history_date]
            total = cust_object['delay'].sum()
            length = len(cust_object)
            if name not in important_customers:
                objects['cust_total_avg_delay'] = -1
                if (length != 0):
                    non_imp_total = non_imp_total + total
                    non_imp_length = non_imp_length + length
            else:
                if (length == 0):
                    objects['cust_total_avg_delay'] = 0
                else:
                    objects['cust_total_avg_delay'] = total / length
                    imp_total = imp_total + total
                    imp_length = imp_length + length
            list_data.append(objects['cust_total_avg_delay'])
        imp_avg = imp_total / imp_length
        non_imp_avg = non_imp_total / non_imp_length
        list1 = []
        for df in list_data:
            list1.append(df.values)
        g1 = []
        for i in list1:
            for k in i:
                g1.append(k)
        d1 = []
        for i in g1:
            if (i == 0):
                i = imp_avg
            if (i == -1):
                i = non_imp_avg
            d1.append(i)
        return d1

    def cust_avg_delay(self):
        grouped = self.groupCustomers()
        important_customers = self.importantCustomers(self.aggregator, self.limit)
        imp_total = 0
        imp_length = 0
        non_imp_total = 0
        non_imp_length = 0
        list_data = []
        for name, objects in grouped:
            cust_object = objects[(objects['delay'] > 0) & (objects['mega_date'] < self.history_date)]
            total = cust_object['delay'].sum()
            length = len(cust_object)
            if name not in important_customers:
                objects['cust_avg_delay'] = -1
                if length != 0:
                    non_imp_total = non_imp_total + total
                    non_imp_length = non_imp_length + length
            else:
                if length == 0:
                    objects['cust_avg_delay'] = 0
                else:
                    objects['cust_avg_delay'] = total / length
                    imp_total = imp_total + total
                    imp_length = imp_length + length
            list_data.append(objects['cust_avg_delay'])
        imp_avg = imp_total / imp_length
        non_imp_avg = non_imp_total / non_imp_length
        list2 = []
        for df in list_data:
            list2.append(df.values)

        g2 = []
        for i in list2:
            for k in i:
                g2.append(k)

        d2 = []
        for i in g2:
            if (i == 0):
                i = imp_avg
            if (i == -1):
                i = non_imp_avg
            d2.append(i)
        return d2

    def cust_delay_payment_percent(self):
        grouped = self.groupCustomers()
        important_customers = self.importantCustomers(self.aggregator, self.limit)
        imp_total = 0
        imp_length = 0
        non_imp_total = 0
        non_imp_length = 0
        list_data = []
        for name, objects in grouped:
            cust_object = objects[objects['mega_date'] < self.history_date]
            total = len(cust_object[cust_object['delay'] > 0])
            length = len(cust_object)
            if name not in important_customers:
                objects['cust_delay_payment_percent'] = -1
                if length != 0:
                    non_imp_total = non_imp_total + total
                    non_imp_length = non_imp_length + length
            else:
                if length == 0:
                    objects['cust_delay_payment_percent'] = 0
                else:
                    objects['cust_delay_payment_percent'] = (total / length) * 100
                    imp_total = imp_total + total
                    imp_length = imp_length + length
            list_data.append(objects['cust_delay_payment_percent'])
        imp_avg = (imp_total / imp_length) * 100
        non_imp_avg = (non_imp_total / non_imp_length) * 100
        print("0-replaced by:", imp_avg)
        print("-1 replaced by: ", non_imp_avg)
        list3 = []
        for dat in list_data:
            list3.append(dat.values)

        g3 = []
        for i in list3:
            for k in i:
                g3.append(k)

        d3 = []
        for i in g3:
            if (i == 0):
                i = imp_avg
            if (i == -1):
                i = non_imp_avg
            d3.append(i)
        return d3

    def cust_avg_gap(self):
        import pandas as pd
        grouped = self.groupCustomers()
        important_customers = self.importantCustomers(self.aggregator, self.limit)
        imp_gap_list = []
        non_imp_gap_list = []
        cust_avg_gap_list = []
        for name, objects in grouped:
            cust_object = objects[objects['mega_date'] < self.history_date]
            unique_clearing_date = cust_object.clearing_date_norm.unique()
            unique_clearing_date = pd.to_datetime(unique_clearing_date)
            unique_clearing_date = unique_clearing_date.sort_values(ascending=True)
            gap = 0
            if name not in important_customers:
                objects['cust_avg_gap'] = -1
                if (len(unique_clearing_date) not in (0, 1)):
                    for i in range(0, len(unique_clearing_date) - 1):
                        j = i + 1
                        diff = (unique_clearing_date[j] - unique_clearing_date[i]).days
                        non_imp_gap_list.append(diff)
            else:
                if (len(unique_clearing_date) == 0 | len(unique_clearing_date) == 1):
                    objects['cust_avg_gap'] = 0
                else:
                    for i in range(0, len(unique_clearing_date) - 1):
                        j = i + 1
                        diff = (unique_clearing_date[j] - unique_clearing_date[i]).days
                        imp_gap_list.append(diff)
                        gap = gap + diff
                    objects['cust_avg_gap'] = gap / (len(unique_clearing_date) - 1)
            cust_avg_gap_list.append(objects['cust_avg_gap'])
        imp_gap = sum(imp_gap_list) / len(imp_gap_list)
        non_imp_gap = sum(non_imp_gap_list) / len(non_imp_gap_list)
        print(sum(imp_gap_list))
        print(len(imp_gap_list))
        print(sum(non_imp_gap_list))
        print(len(non_imp_gap_list))
        print("0-replaced by:", imp_gap)
        print("-1 replaced by: ", non_imp_gap)
        list11 = []
        for data1 in cust_avg_gap_list:
            list11.append(data1.values)

        g = []
        for i in list11:
            for k in i:
                g.append(k)

        d = []
        for i in g:
            if (i == 0):
                i = imp_gap
            if (i == -1):
                i = non_imp_gap
            d.append(i)
        return d

    def for_a(self):
        grouped = self.groupCustomers()
        important_customers = self.importantCustomers(self.aggregator, self.limit)
        imp_total = 0
        imp_len = 0
        non_imp_total = 0
        non_imp_len = 0
        list_data = []
        for name, objects in grouped:
            cust_object = objects[objects['mega_date'] < self.history_date]
            total = cust_object['invoice_amount_norm'].sum()
            length = len(cust_object)
            if name not in important_customers:
                objects['for_a'] = -1
                if length != 0:
                    non_imp_total = non_imp_total + total
                    non_imp_len = non_imp_len + length
            else:
                if length == 0:
                    objects['for_a'] = 0
                else:
                    objects['for_a'] = total / length
                    imp_total = imp_total + total
                    imp_len = imp_len + length
            list_data.append(objects['for_a'])
        imp_avg = imp_total / imp_len
        non_imp_avg = non_imp_total / non_imp_len
        list11 = []
        for t in list_data:
            list11.append(t.values)

        g = []
        for i in list11:
            for k in i:
                g.append(k)

        d = []
        for i in g:
            if (i == 0):
                i = imp_avg
            if (i == -1):
                i = non_imp_avg
            d.append(i)
        return d

    def delay_avg(self):
        grouped = self.groupCustomers()
        important_customers = self.importantCustomers(self.aggregator, self.limit)
        imp_total = 0
        imp_length = 0
        non_imp_total = 0
        non_imp_length = 0
        list_data = []
        for name, objects in grouped:
            cust_object = objects[(objects['mega_date'] < self.history_date) & (objects['delay'] > 0)]
            total = cust_object['invoice_amount_norm'].sum()
            length = len(cust_object)
            if name not in important_customers:
                objects['delay_avg'] = -1
                if length != 0:
                    non_imp_total = non_imp_total + total
                    non_imp_length = non_imp_length + length
            else:
                if length == 0:
                    objects['delay_avg'] = 0
                else:
                    objects['delay_avg'] = total / length
                    imp_total = imp_total + total
                    imp_length = imp_length + length
            list_data.append(objects['delay_avg'])
        sum_average = imp_total / imp_length
        non_imp_avg = non_imp_total / non_imp_length
        print("0-replaced by:", sum_average)
        print("-1 replaced by: ", non_imp_avg)
        list11 = []
        for df in list_data:
            list11.append(df.values)

        g = []
        for i in list11:
            for k in i:
                g.append(k)

        d = []
        for i in g:
            if (i == 0):
                i = sum_average
            if (i == -1):
                i = non_imp_avg
            d.append(i)
        return d

    def derived_features(self):
        import numpy as np
        self.data['a'] = np.where(self.data['for_a'] == 0, 0, self.data['delay_avg'] / self.data['for_a'])
        self.data['b'] = np.where((self.data['a'] > 1) & (self.data['delay_avg'] != 0),self.data['invoice_amount_norm'] / self.data['delay_avg'],self.data['delay_avg'] / self.data['invoice_amount_norm'])
        self.data['invoice_sum_ratio'] = np.where(self.data['cust_avg_sum'] == 0, 0,self.data['invoice_amount_norm'] / self.data['cust_avg_sum'])
        return self.data

    def init(self):
        self.data['delay_ratio'] = 0
        self.data['delay_sum_ratio'] = 0
        self.data['a'] = 0
        self.data['b'] = 0
        self.data['invoice_sum_ratio'] = 0
        self.data['batch_size'] = 0
        self.data['bat_size'] = 0
        self.data['cust_avg_sum'] = 0
        self.data['cust_total_avg_delay'] = 0
        self.data['cust_avg_delay'] = 0
        self.data['cust_avg_gap'] = 0
        self.data['for_a'] = 0
        self.data['delay_avg'] = 0
        self.data['cust_delay_payment_percent'] = 0

    def CLCalculations(self):
        self.init()
        print('Calculating Delay Ratio')
        self.data['delay_ratio'] = self.delay_ratio()
        print('Calculating Delay Sum Ratio')
        self.data['delay_sum_ratio'] = self.delay_sum_ratio()
        print('Calculating Batch Size')
        self.data['batch_size'] = self.batch_size()
        print('Calculating Bat Size')
        self.data['bat_size'] = self.bat_size()
        print('Calculating Customer Average Sum')
        self.data['cust_avg_sum'] = self.cust_avg_sum()
        print('Calculating Customer Total Average Delay')
        self.data['cust_total_avg_delay'] = self.cust_total_avg_delay()
        print('Calculating Customer Average Delay')
        self.data['cust_avg_delay'] = self.cust_avg_delay()
        print('Calculating Customer Average Gap')
        self.data['cust_avg_gap'] = self.cust_avg_gap()
        print('Calculating For A')
        self.data['for_a'] = self.for_a()
        print('Calculating Delay Average')
        self.data['delay_avg'] = self.delay_avg()
        print('Calculating Customer Delay Payment Percent')
        self.data['cust_delay_payment_percent'] = self.cust_delay_payment_percent()
        print('Calculating Derived Features')
        self.data = self.derived_features()
        return self.data
