import os
from datetime import datetime
from pyspark.sql import Window, functions as F
from pyspark.sql.types import TimestampType
from utils import visit_datetime_normalize_udf


class MarketingModelETLPipeline:
    def __init__(self, spark_session, user_df, visitor_logs_df, start_date, end_date, output_dir):
        self.spark_session = spark_session
        self.user_df = user_df
        self.visitor_logs_df = visitor_logs_df
        self.start_date = start_date
        self.end_date = end_date
        self.output_dir = output_dir

        # Make utils available on all the worker nodes
        utils_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'utils.py')
        self.spark_session.sparkContext.addPyFile(utils_path)

    def load(self, df):
        df.write.csv(os.path.join(self.output_dir), 'pipeline_output.csv')

    def _preprocess_visitor_logs(self):
        """
        Create VisitDateTime_normalized column, typecast to timestamp type and sort entire df by VisitDateTime_normalized
        """
        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'VisitDateTime_normalized', visit_datetime_normalize_udf('VisitDateTime')
        )
        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'VisitDateTime_normalized', F.col('VisitDateTime_normalized').cast(TimestampType())
        )
        self.visitor_logs_df = self.visitor_logs_df.sort('VisitDateTime_normalized')

    def _fill_null_visit_datetime(self):
        """
        Fill null VisitDateTime_normalized rows by taking the first value of webClientID and ProductID combination
        """
        w = Window.partitionBy(self.visitor_logs_df.webClientID, self.visitor_logs_df.ProductID)
        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'first_webClientID_ProductID', F.first(
                self.visitor_logs_df.VisitDateTime_normalized, ignorenulls=True
            ).over(w)
        )
        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'VisitDateTime_normalized_na_filled',
            F.when(
                F.col('VisitDateTime_normalized').isNull(),
                F.col('first_webClientID_ProductID')
            ).otherwise(F.col('VisitDateTime_normalized'))
        )

    def _filter_visitor_logs(self):
        """
        Filter the logs according to the daterange passed
        :return:
        """
        filtered_visitor_logs = self.visitor_logs_df\
            .filter(self.visitor_logs_df.VisitDateTime_normalized_na_filled >= datetime.strptime(self.start_date, '%Y-%m-%d'))\
            .filter(self.visitor_logs_df.VisitDateTime_normalized_na_filled <= datetime.strptime(self.end_date, '%Y-%m-%d'))

        return filtered_visitor_logs

    def _preprocess(self):
        filtered_visitor_logs_path = os.path.join(self.output_dir, 'filtered_visitor_logs')
        if os.path.exists(filtered_visitor_logs_path):
            print(f'Reading from filtered_visitor_logs stored in {self.output_dir}')
            filtered_visitor_logs = self.spark_session.read.parquet(filtered_visitor_logs_path)
        else:
            print('Preprocessing visitor logs')
            self._preprocess_visitor_logs()
            self._fill_null_visit_datetime()
            filtered_visitor_logs = self._filter_visitor_logs()
            filtered_visitor_logs.write.parquet(filtered_visitor_logs_path)

    def run(self):
        self._preprocess()
        # self.user_df.show(truncate=False)
        # self.visitor_logs_df.show(truncate=False)
