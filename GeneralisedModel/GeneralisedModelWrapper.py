from FeatureCreationRunner import FeatureCreationRunner
class GeneralisedModelWrapper(FeatureCreationRunner):

    def __init__(self,data,history_date,train_date,aggregator,customer_identifier):
        super().__init__(data,history_date,train_date,aggregator,customer_identifier)

    def runFeatureCreation(self):
        self.run()