""" Features for AccountingEstablished """
from common.common_feature import CommonFeature

class AccountingEstablished(CommonFeature):
    """ extract features for AccountingEstablished """
    def __init__(self, spark, conf):
        CommonFeature.__init__(self, spark, conf)
        self.select_columns = list(set(conf.get('predictors') + [self.label] + self.unique_id))

    def get_window_data(self, reference_date):
        """ extract features for AccountingEstablished """
        ac_out = self.get_contract(reference_date)
        aa_out = self.get_active_accountant(ac_out, reference_date)
        pv_out = self.get_pageviews(aa_out, reference_date)
        df_out = self.get_divisionstatistics_financial(pv_out, reference_date)
        output_df = self.get_label(df_out, reference_date)
        return output_df.select(self.select_columns)
