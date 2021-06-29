import os
from pyspark.sql import Window, functions as F
from pyspark.sql.types import TimestampType
from utils import visit_datetime_normalize_udf


class MarketingModelETLPipeline:
    def __init__(self, spark_session, user_df, visitor_logs_df, start_date, end_date, output_path):
        self.spark_session = spark_session
        self.user_df = user_df
        self.visitor_logs_df = visitor_logs_df
        self.start_date = start_date
        self.end_date = end_date
        self.output_path = output_path

        # Make utils available on all the worker nodes
        utils_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'utils.py')
        self.spark_session.sparkContext.addPyFile(utils_path)

    def load(self, df):
        df.write.csv(self.output_path)

    def _preprocess_visitor_logs(self):
        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'VisitDateTime_normalized', visit_datetime_normalize_udf('VisitDateTime')
        )
        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'VisitDateTime_normalized', F.col('VisitDateTime_normalized').cast(TimestampType())
        )
        self.visitor_logs_df = self.visitor_logs_df.sort('VisitDateTime_normalized')

    def _fill_null_visit_datetime(self):
        w = Window.partitionBy(self.visitor_logs_df.webClientID, self.visitor_logs_df.ProductID)
        res = self.visitor_logs_df.withColumn(
            "first_webClientID_ProductID", F.first(
                self.visitor_logs_df.VisitDateTime_normalized, ignorenulls=True
            ).over(w)
        )
        x = res.withColumn("VisitDateTime_normalized_na_filled", when(col("VisitDateTime_normalized").isNull(), col("first_webClientID_ProductID")).otherwise(col("VisitDateTime_normalized")))
        x.show()
        print(x.count())
        null_x = self.visitor_logs_df.where(F.col('VisitDateTime_normalized_na_filled').isNull())
        null_x.show()
        print(null_x.count())

    def _filter_visitor_logs(self):
        pass

    def _preprocess(self):
        self._preprocess_visitor_logs()
        self._fill_null_visit_datetime()
        self._filter_visitor_logs()

    def run(self):
        self._preprocess()
        # self.user_df.show(truncate=False)
        # self.visitor_logs_df.show(truncate=False)
