from CustomerLevelFeatures import CustomerLevelFeatures
from InvoiceLevelFeatures import InvoiceLevelFeatures
import pandas as pd
class FeatureCreationRunner(CustomerLevelFeatures,InvoiceLevelFeatures):

    def __init__(self,data,history_date,train_date,aggregator,customer_identifier,limit):
        CustomerLevelFeatures.__init__(self,data,history_date,train_date,aggregator,limit)
        InvoiceLevelFeatures.__init__(self,data,history_date,train_date,customer_identifier)

    def run(self):
        print('Running Customer Level Calculations')
        self.CLCalculations()
        print('Finishing Customer Level Calculations')
        print('Running Invoice Level Calculations')
        self.ILCalculations()
        print('Finishing Invoice Level Calculations')
        self.data.to_csv(r'E:\OutputFeatureCreation.csv')
        self.train_test.to_csv(r'E:\OutputFeatureCreation_TT.csv')

