import os
import argparse
from pyspark.sql import SparkSession
from pipeline import MarketingModelETLPipeline


def run_pipeline(user_data_input_path, visitor_logs_data_input_path, start_date, end_date, pipeline_output_dir):
    # Create spark session
    spark_session = (
        SparkSession.builder
        .master('local[*]')
        .appName('MarketingModelETLPipeline')
        .getOrCreate()
    )

    # Create user and visitor logs dataframes using the paths
    user_df = spark_session.read.csv(user_data_input_path, header=True)
    visitor_logs_df = spark_session.read.csv(visitor_logs_data_input_path, header=True)

    # Get the pipeline's output by calling the run method
    pipeline = MarketingModelETLPipeline(
        spark_session, user_df, visitor_logs_df, start_date, end_date, pipeline_output_dir
    )
    output_df = pipeline.run()

    # Dump the output in a csv
    pipeline.load(output_df)


if __name__ == '__main__':
    # Read input paths and dates from input args
    parser = argparse.ArgumentParser(description='Date inputs for Marketing Model ETL Pipeline')

    default_user_data_input_path = os.path.join(os.path.realpath(__file__), '../../data/userTable.csv')
    parser.add_argument(
        '--user_data_input_path', required=False, type=str, default=default_user_data_input_path,
        help='User Data csv path'
    )

    default_visitor_logs_data_input_path = os.path.join(os.path.realpath(__file__), '../../data/VisitorLogsData.csv')
    parser.add_argument(
        '--visitor_logs_data_input_path', type=str, default=default_visitor_logs_data_input_path,
        help='Visitor Logs Data csv path'
    )

    default_pipeline_output_dir = os.path.join(os.path.realpath(__file__), '../../data/')
    parser.add_argument(
        '--pipeline_output_dir', type=str, default=default_pipeline_output_dir, help='Pipeline output csv path'
    )

    parser.add_argument('--start_date', type=str, default='2018-05-07', help='Start date for Visitor Logs Data')
    parser.add_argument('--end_date', type=str, default='2018-05-27', help='End date for Visitor Logs Data')
    args = parser.parse_args()

    # Run the pipeline by passing the input paths and date range
    run_pipeline(
        args.user_data_input_path, args.visitor_logs_data_input_path, args.start_date, args.end_date,
        args.pipeline_output_dir
    )
