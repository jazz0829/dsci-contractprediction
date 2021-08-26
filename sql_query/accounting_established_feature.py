class AccountingEstablishedFeature():
    def __init__(self, reference_date, config):
        self.reference_date = reference_date
        self.config = config
        self.features = None
    
    def get_accounting_established_feature(self, cf_obj): 
        # contract
        cf_obj.get_contract()     
        cf_obj.features = cf_obj.features[cf_obj.features.TenureInMonths >= 6]
        # division statistics financial
        cf_obj.get_divisionstatistics_financial()  
        # active accountant
        cf_obj.get_active_accountant() 
        # pageviews
        cf_obj.get_pageviews()
        # label        
        cf_obj.get_label()
        
        self.features = cf_obj.features
                
        return self.features
        
    