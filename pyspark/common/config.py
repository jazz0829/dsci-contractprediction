""" configurations """

from common.accounting_established import AccountingEstablished

C_ACCOUNTING_ESTABLISHED = {
    "product":"Accounting",
    "phase": "Established",
    "country":"NL,BE",
    "period": 3,
    "window_activities": 91,
    "window_contract": 91,
    "algorithm": "LogisticRegression",
    "accepted_auc": 0.8,
    "predictors":  ["log_pageviews", "log_bank_entry_count", "log_cash_entry_count", "accountant",
                    "log_sales_entry_count", "log_purchase_entry_count", "log_general_journal_entry_count",
                    "entrepreneur_active", "log_tenure_upgrade_in_months", "environment"],
    "label": "churned",
    "dataset_obj": AccountingEstablished,
    "training_data_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/train/accounting_established",
    "model_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/model/accounting_established",
    "statistics_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/statistics/accounting_established",
    "predictions_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/prediction/accounting_established"}

C_ACCOUNTING_ONBOARDING = {
    "product":"Accounting",
    "phase": "Onboarding",
    "country":"NL,BE",
    "period": 3,
    "window_activities": 91,
    "window_contract": 91,
    "algorithm": "LogisticRegression",
    "accepted_auc": 0.75,
    "predictors":  ["tenure_in_months", "accountant", "master_data_setup_type", "log_bank_entry_count",
                    "log_cash_entry_count", "log_account_count", "log_pageviews", "log_sales_entry_count",
                    "log_purchase_entry_count", "has_currency_count", "log_general_journal_entry_count",
                    "entrepreneur_active", "environment"],
    "label": "churned",
    "training_data_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/train/accounting_onboarding",
    "model_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/model/accounting_onboarding",
    "statistics_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/statistics/accounting_onboarding",
    "predictions_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/prediction/accounting_onboarding"}

C_ACCOUNTANCY = {
    "product":"Accountancy",
    "phase": "All",
    "country":"NL,BE",
    "period": 3,
    "window_activities": 91,
    "window_contract": 91,
    "algorithm": "LogisticRegression",
    "accepted_auc": 0.8,
    "predictors":  ["login_freq", "log_bank_entry_count", "log_cash_entry_count", "log_sales_entry_count",
                    "log_purchase_entry_count", "log_general_journal_entry_count",
                    "log_other_accoutancy_activities", "tenure_in_months_upgrade", "log_num_activated_divisions",
                    "log_num_active_linked_companies", "log_pageviews_on_linked_companies",
                    "log_time_sales_invoice_entry"],
    "label": "churned",
    "training_data_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/train/accountancy",
    "model_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/model/accountancy",
    "statistics_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/statistics/accountancy",
    "predictions_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/prediction/accountancy"}

C_MANUFACTURING = {
    "product":"Manufacturing",
    "phase": "All",
    "country":"NL",
    "period": 3,
    "window_activities": 30,
    "window_contract": 91,
    "algorithm": "LogisticRegression",
    "accepted_auc": 0.83,
    "predictors":  ["tenure_in_months_upgrade", "recent_use_mobile", "login_freq", "has_bank_link",
                    "log_bank_entry_count", "has_sales_invoice", "log_sales_entry_count",
                    "log_purchase_entry_count", "log_manufacturing_activities"],
    "label": "churned",
    "training_data_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/train/manufacturing",
    "model_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/model/manufacturing",
    "statistics_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/statistics/manufacturing",
    "predictions_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/prediction/manufacturing"}


C_PROFESSIONAL_SERVICES = {
    "product":"Professional services",
    "phase": "All",
    "country":"NL,BE",
    "period": 3,
    "window_activities": 30,
    "window_contract": 91,
    "algorithm": "LogisticRegression",
    "accepted_auc": 0.8,
    "predictors":  ["recent_use_mobile", "login_freq", "has_sales_invoice", "log_sales_entry_count",
                    "log_bank_entry_count", "log_projects_count", "log_project_time_cost_entries_count",
                    "environment"],
    "label": "churned",
    "training_data_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/train/professional_services",
    "model_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/model/professional_services",
    "statistics_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/statistics/professional_services",
    "predictions_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/prediction/professional_services"}

C_WHOLESALE_DISTRIBUTION = {
    "product":"Wholesale distribution",
    "phase": "All",
    "country":"NL,BE",
    "period": 3,
    "window_activities": 30,
    "window_contract": 91,
    "algorithm": "LogisticRegression",
    "accepted_auc": 0.83,
    "predictors":  ["tenure_in_months_upgrade", "recent_use_mobile", "login_freq", "accountant_or_linked",
                    "active_users", "log_bank_entry_count", "log_cash_entry_count", "log_quote_count",
                    "log_stock_count_entry_count", "log_sales_entry_count", "log_purchase_entry_count",
                    "environment"],
    "label": "churned",
    "training_data_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/train/wholesale_distribution",
    "model_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/model/wholesale_distribution",
    "statistics_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/statistics/wholesale_distribution",
    "predictions_path" : "s3://dt-dsci-contract-prediction-131239767718-eu-west-1/churn/prediction/wholesale_distribution"}

D_PROFESSIONAL_SERVICES = {
    "product":"Professional services",
    "phase": "All",
    "country":"NL,BE",
    "period": 3,
    "window_activities": 30,
    "window_contract": 91,
    "algorithm": "LogisticRegression",
    "accepted_auc": 0.80,
    "predictors":  ["recent_use_mobile", "has_sales_invoice", "log_projects_count",
                    "log_project_time_cost_entries_count", "bank_link", "big3_bank_account", "has_bank_link",
                    "has_big3_bank_account", "log_tenure_in_months", "log_tenure_in_months_upgrade",
                    "project_type_prepaid_htb"],
    "label": "downgraded"
}

D_WHOLESALE_DISTRIBUTION = {
    "product":"Wholesale distribution",
    "phase": "All",
    "country":"NL",
    "period": 3,
    "window_activities": 30,
    "window_contract": 91,
    "algorithm": "LogisticRegression",
    "accepted_auc": 0.82,
    "predictors":  ["tenure_in_months_upgrade", "recent_use_mobile", "login_freq",
                    "accountant_or_linked", "active_users", "sector",
                    "has_bank_link", "has_wholesale_name", "log_quote_count", "log_account_count",
                    "log_stock_count_entry_count", "log_item_count", "log_sales_order_entry_count",
                    "log_purchase_order_entry_count"],
    "label": "downgraded"
}


CHURN_CONFIG = {
    'accounting_established': C_ACCOUNTING_ESTABLISHED,
    'accounting_onboarding': C_ACCOUNTING_ESTABLISHED,
    'accountancy': C_ACCOUNTANCY,
    'manufacturing': C_MANUFACTURING,
    'professional_services': C_PROFESSIONAL_SERVICES,
    'wholesale_distribution': C_WHOLESALE_DISTRIBUTION}
DOWNGRADE_CONFIG = {
    'professional_services': D_PROFESSIONAL_SERVICES,
    'wholesale_distribution': D_WHOLESALE_DISTRIBUTION}

CONFIG = {'churn': CHURN_CONFIG,
          'downgrade': DOWNGRADE_CONFIG}
