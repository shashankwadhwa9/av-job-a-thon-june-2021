import os
from datetime import datetime, timedelta
from pyspark.sql import Window, functions as F
from pyspark.sql.types import TimestampType
from utils import visit_datetime_normalize_udf


class MarketingModelETLPipelinePreprocessor:
    def __init__(self, spark_session, visitor_logs_df, start_date, end_date, output_dir):
        self.spark_session = spark_session
        self.visitor_logs_df = visitor_logs_df
        self.start_date = start_date
        self.end_date = end_date
        self.output_dir = output_dir

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
        self.visitor_logs_df = self.visitor_logs_df.withColumn('ProductID', F.lower(F.col('ProductID')))

    def _fill_null_visit_datetime(self):
        """
        Fill null VisitDateTime_normalized rows by taking the first value of webClientID and ProductID combination
        """
        # Take first of webClientID and ProductID
        w = Window \
            .partitionBy(self.visitor_logs_df.webClientID, self.visitor_logs_df.ProductID) \
            .orderBy(F.col('VisitDateTime_normalized').asc_nulls_last())

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

        # Take first of UserID
        w = Window \
            .partitionBy(self.visitor_logs_df.UserID) \
            .orderBy(F.col('VisitDateTime_normalized_na_filled').asc_nulls_last())

        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'first_UserID', F.first(
                self.visitor_logs_df.VisitDateTime_normalized_na_filled, ignorenulls=True
            ).over(w)
        )
        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'VisitDateTime_normalized_na_filled',
            F.when(
                F.col('VisitDateTime_normalized_na_filled').isNull(),
                F.col('first_UserID')
            ).otherwise(F.col('VisitDateTime_normalized_na_filled'))
        )

    def _filter_visitor_logs(self):
        """
        Filter the logs according to the daterange passed
        """
        self.visitor_logs_df = self.visitor_logs_df \
            .filter(self.visitor_logs_df.VisitDateTime_normalized_na_filled >= datetime.strptime(self.start_date, '%Y-%m-%d')) \
            .filter(self.visitor_logs_df.VisitDateTime_normalized_na_filled < datetime.strptime(self.end_date, '%Y-%m-%d'))

    def _fill_null_activity(self):
        """
        Fill null Activity rows by taking the previous value of webClientID
        """
        window = Window \
            .partitionBy(self.visitor_logs_df.webClientID) \
            .orderBy(self.visitor_logs_df.VisitDateTime_normalized_na_filled)

        self.visitor_logs_df = self.visitor_logs_df.withColumn('lag_Activity', F.lag('Activity', 1).over(window))
        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'Activity_na_filled',
            F.when(
                F.col('Activity').isNull(),
                F.col('lag_Activity')
            ).otherwise(F.col('Activity'))
        )

    def _fill_null_product_id(self):
        """
        Fill null ProductID rows by taking the previous value of webClientID
        """
        window = Window \
            .partitionBy(self.visitor_logs_df.webClientID) \
            .orderBy(self.visitor_logs_df.VisitDateTime_normalized_na_filled)

        self.visitor_logs_df = self.visitor_logs_df.withColumn('lag_ProductID', F.lag('ProductID', 1).over(window))
        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'ProductID_na_filled',
            F.when(
                F.col('ProductID').isNull(),
                F.col('lag_ProductID')
            ).otherwise(F.col('ProductID'))
        )

    def _convert_to_lowercase(self):
        """
        Convert Activity and OS values to lowercase
        """
        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'Activity_na_filled', F.lower(F.col('Activity_na_filled'))
        )
        self.visitor_logs_df = self.visitor_logs_df.withColumn('OS', F.lower(F.col('OS')))

    def _fill_null_leftovers(self):
        """
        Fill null values of Activity and ProductID rows by taking the most common value of the user
        """
        # Activity
        grouped = self.visitor_logs_df \
            .filter(F.col('Activity_na_filled').isNotNull()) \
            .groupBy('UserID', 'Activity_na_filled').count()

        window = Window.partitionBy('UserID').orderBy(F.desc('count'))
        df_most_frequent_activity = grouped \
            .withColumn('row_number', F.row_number().over(window)) \
            .where(F.col('row_number') == 1) \
            .select('UserID', 'Activity_na_filled') \
            .withColumnRenamed('Activity_na_filled', 'Most_Frequent_Activity')

        self.visitor_logs_df = self.visitor_logs_df.join(df_most_frequent_activity, 'UserID', how='left')
        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'Activity_na_filled',
            F.when(
                F.col('Activity_na_filled').isNull(),
                F.col('Most_Frequent_Activity')
            ).otherwise(F.col('Activity_na_filled'))
        )

        # ProductID
        grouped = self.visitor_logs_df \
            .filter(F.col('ProductID_na_filled').isNotNull()) \
            .groupBy('UserID', 'ProductID_na_filled').count()

        window = Window.partitionBy('UserID').orderBy(F.desc('count'))
        df_most_frequent_product = grouped \
            .withColumn('row_number', F.row_number().over(window)) \
            .where(F.col('row_number') == 1) \
            .select('UserID', 'ProductID_na_filled') \
            .withColumnRenamed('ProductID_na_filled', 'Most_Frequent_Product')

        self.visitor_logs_df = self.visitor_logs_df.join(df_most_frequent_product, 'UserID', how='left')
        self.visitor_logs_df = self.visitor_logs_df.withColumn(
            'ProductID_na_filled',
            F.when(
                F.col('ProductID_na_filled').isNull(),
                F.col('Most_Frequent_Product')
            ).otherwise(F.col('ProductID_na_filled'))
        )

    def preprocess(self):
        filtered_visitor_logs_path = os.path.join(self.output_dir, 'filtered_visitor_logs')
        # Check if preprocessed data is already present on the disk
        if os.path.exists(filtered_visitor_logs_path):
            print(f'Reading from filtered_visitor_logs stored in {self.output_dir}')
            self.visitor_logs_df = self.spark_session.read.parquet(filtered_visitor_logs_path)
        else:
            print('Preprocessing visitor logs')
            self._preprocess_visitor_logs()
            self._fill_null_visit_datetime()
            self._filter_visitor_logs()
            self._fill_null_activity()
            self._fill_null_product_id()
            self._convert_to_lowercase()
            self._fill_null_leftovers()
            self.visitor_logs_df.write.parquet(filtered_visitor_logs_path)

        return self.visitor_logs_df
