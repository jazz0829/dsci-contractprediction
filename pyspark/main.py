'''
orchestration steps
'''

import argparse
import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from slackclient import SlackClient
import common.config as config
from common.accounting_established import AccountingEstablished
from common.model_operation import ModelOperation
import common.utils as utils

SLACK_TOKEN = "xoxb-28674668403-605322818656-UC8IGyHQo40z4IhwvhrG3lPe"
PROJECTNAME = 'HealthScorePredictions'
SCRIPTNAME = 'main.py'

def send_to_slack(message, target_channel="GHQKCTSNN"): # cig-lago-log
    """
    send to slack function
    """
    try:
        message = str(message)
        print(message)
        slackclient = SlackClient(SLACK_TOKEN)
        slackclient.api_call(
            "chat.postMessage",
            channel=target_channel,
            text="%s: %s- %s"%(PROJECTNAME, SCRIPTNAME, message))
    except Exception as general_exception:
        print("cannot send on slack")
        print(general_exception)

def run_operations(spark, category, mode, config_name, current_date):
    """ orechestration set up """
    # extend with generalized columns
    config_json = config.CONFIG[category][config_name]
    config_json['unique_id'] = ["environment", "account_code", "package_code"]
    config_json['mode'] = mode
    config_json['category'] = category
    config_json['current_date'] = current_date
    config_json['past_date'] = current_date - relativedelta(days=config_json.get('window_contract'))

    # create objects
    model_ops_obj = ModelOperation(spark, config_json)
    dataset_obj = config_json['dataset_obj'](spark, config_json)

    # operations
    if (mode == 'train' or mode == 'all'):
        train_df = dataset_obj.get_dataset(current_date, mode)
        train_df.repartition(1).write.mode('overwrite').parquet(config_json.get('training_data_path'))
        auc, reg_param = model_ops_obj.select_and_train()
        utils.write_train_statistics(spark, config_json, auc, reg_param)
    elif (mode == 'predict' or mode == 'all'):
        flag = False #TODO check whether healthscore already exists or not
        if not flag:
            test_df = dataset_obj.get_dataset(current_date, mode)
            scores = model_ops_obj.predict(test_df)
            utils.write_churn_output(config_json, scores)
        else:
            print('Health scores for {} already exist'.format(current_date)) #TODO dump into logfile
    elif (mode == 'evaluate' or mode == 'all'):
        pass
    elif (mode == 'validate' or mode == 'all'):
        pass

def main():
    """ entry point, triggers model operation tasks.
    """
    parser = argparse.ArgumentParser("OPERATIONS")
    parser.add_argument("--category", '-hc', metavar="category", type=str, required=True, help="churn or downgrade")
    parser.add_argument("--mode", '-hm', metavar="mode", type=str, required=True, help="train or predict or validate or evaluate")
    parser.add_argument("--config_name", '-hn', metavar="config_name", type=str, required=True, help="accounting_established")
    parser.add_argument("--current_date", '-hd', metavar="current_date", type=datetime, required=True, help="currentdate")
    conf = parser.parse_args()
    arg_category = conf.category
    arg_mode = conf.mode
    arg_config_name = conf.config_name
    arg_current_date = conf.current_date
    spark = SparkSession.builder.appName('HealthScoreTrainingAndPrediction').getOrCreate()
    try:
        run_operations(spark, arg_category, arg_mode, arg_config_name, arg_current_date)
    except Exception as general_exception:
        send_to_slack('Error during ETL')
        send_to_slack(general_exception)
        print(1 + 'a')

    spark.stop()

if __name__ == '__main__':
    main()
