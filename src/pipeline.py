import os
from pyspark.sql import functions as F
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
        # self.visitor_logs_df = self.visitor_logs_df.sort('VisitDateTime_normalized')

    def _fill_null_visit_datetime(self):
        both = 0
        one = 0
        none = 0

        null_rows = self.visitor_logs_df.where(F.col('VisitDateTime_normalized').isNull())
        for row in null_rows.rdd.collect():
            print(row)
            first_similar_row = self.visitor_logs_df.where(
                (F.col('VisitDateTime_normalized').isNotNull()) &
                (F.col('webClientID') == row['webClientID']) &
                (F.col('ProductID') == row['ProductID'])
            ).take(1)
            if len(first_similar_row) == 1:
                print('match found with webClientID and ProductID')
                print(first_similar_row[0])
                both += 1
            else:
                first_similar_row = self.visitor_logs_df.where(
                    (F.col('VisitDateTime_normalized').isNotNull()) &
                    (F.col('webClientID') == row['webClientID'])
                ).take(1)

                if len(first_similar_row) == 1:
                    print('match found with webClientID')
                    print(first_similar_row[0])
                    one += 1
                else:
                    print('no match found')
                    none += 1

        print(both, one, none)

    def _filter_visitor_logs(self):
        # print(self.visitor_logs_df.count())
        # self.visitor_logs_df.where(F.col('VisitDateTime_normalized').isNull()).show(truncate=False)
        # c = self.visitor_logs_df.where(F.col('VisitDateTime_normalized').isNull()).count()
        # print(c)
        # self.visitor_logs_df.where(F.col('webClientID') == 'WI100000280033').show(100, False)
        # print(self.visitor_logs_df.where(F.col('webClientID') == 'WI100000280033').count())
        pass

    def _preprocess(self):
        self._preprocess_visitor_logs()
        self._fill_null_visit_datetime()
        self._filter_visitor_logs()

    def run(self):
        self._preprocess()
        self.user_df.show(truncate=False)
        self.visitor_logs_df.show(truncate=False)
