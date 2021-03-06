import os
from datetime import datetime, timedelta
from pyspark.sql import Window, functions as F
from pyspark.sql.types import TimestampType
from preprocessing import MarketingModelETLPipelinePreprocessor


class MarketingModelETLPipeline:
    def __init__(self, spark_session, user_df, visitor_logs_df, start_date, end_date, output_dir):
        self.spark_session = spark_session
        self.user_df = user_df
        self.visitor_logs_df = visitor_logs_df
        self.visitor_logs_df_orig = visitor_logs_df
        self.start_date = start_date
        self.end_date = end_date
        self.output_dir = output_dir

        # Make utils available on all the worker nodes
        utils_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'utils.py')
        self.spark_session.sparkContext.addPyFile(utils_path)
        self.spark_session.conf.set('spark.sql.session.timeZone', 'UTC')

        # Preprocessor
        self.preprocessor = MarketingModelETLPipelinePreprocessor(
            self.spark_session, self.visitor_logs_df, self.start_date, self.end_date, self.output_dir
        )

    def run(self):
        # Get preprocessed data
        self.visitor_logs_df = self.preprocessor.preprocess()

        # Merge user data with visitor logs
        merged_df = self.user_df.join(self.visitor_logs_df, ['UserID'], how='left')
        merged_df = merged_df.withColumn('Signup Date', F.col('Signup Date').cast(TimestampType()))
        merged_df.cache()

        # Compute No_of_days_Visited_7_Days
        cutoff_date = datetime.strptime(self.end_date, '%Y-%m-%d') - timedelta(days=7)
        df = merged_df.filter(merged_df.VisitDateTime_normalized_na_filled >= cutoff_date)
        df_days_visited_7_Days = df.withColumn(
            'VisitDateTime_date', F.to_date(F.col('VisitDateTime_normalized_na_filled'))
        ).groupby('UserID').agg(F.countDistinct('VisitDateTime_date').alias('No_of_days_Visited_7_Days'))

        # Compute No_Of_Products_Viewed_15_Days
        cutoff_date = datetime.strptime(self.end_date, '%Y-%m-%d') - timedelta(days=15)
        df = merged_df.filter(merged_df.VisitDateTime_normalized_na_filled >= cutoff_date)
        df_total_products_viewed_15_Days = df.groupby('UserID').agg(
            F.countDistinct('ProductID_na_filled').alias('No_Of_Products_Viewed_15_Days')
        )

        # Compute User_Vintage
        df_user_vintage = self.user_df.withColumn(
            'User_Vintage', F.datediff(F.to_date(F.lit('2018-05-28')), F.to_date('Signup Date'))
        ).select('UserID', 'User_Vintage')

        # Compute Most_Viewed_product_15_Days
        cutoff_date = datetime.strptime(self.end_date, '%Y-%m-%d') - timedelta(days=15)
        df = merged_df.filter(
            (merged_df.VisitDateTime_normalized_na_filled >= cutoff_date) &
            (merged_df.Activity_na_filled == 'pageload') &
            (merged_df.ProductID_na_filled.isNotNull())
        )
        df_products_viewed_15_Days = df.groupby(['UserID', 'ProductID_na_filled']).agg(
            F.count('Activity_na_filled').alias('cnt'),
            F.max('VisitDateTime_normalized_na_filled').alias('most_recent_visit'),
        )
        window = Window.partitionBy('UserID').orderBy(
            [df_products_viewed_15_Days.cnt.desc(), df_products_viewed_15_Days.most_recent_visit.desc()]
        )
        df_products_viewed_15_Days = df_products_viewed_15_Days.withColumn('row_number', F.row_number().over(window))
        df_most_viewed_product_15_days = df_products_viewed_15_Days\
            .filter(F.col('row_number') == 1)\
            .select('UserID', 'ProductID_na_filled')\
            .withColumnRenamed('ProductID_na_filled', 'Most_Viewed_product_15_Days')

        # Compute Most_Active_OS
        grouped = merged_df.groupBy('UserID', 'OS').count()
        window = Window.partitionBy('UserID').orderBy(F.desc('count'))
        df_most_active_os = grouped\
            .withColumn('row_number', F.row_number().over(window))\
            .where(F.col('row_number') == 1)\
            .select('UserID', 'OS')\
            .withColumnRenamed('OS', 'Most_Active_OS')

        # Compute Recently_Viewed_Product
        df = merged_df.filter(
            (merged_df.Activity_na_filled == 'pageload') &
            (merged_df.ProductID_na_filled.isNotNull())
        )
        df_products_viewed = df.groupby(['UserID', 'ProductID_na_filled']).agg(
            F.max('VisitDateTime_normalized_na_filled').alias('most_recent_visit')
        )
        window = Window.partitionBy('UserID').orderBy(df_products_viewed.most_recent_visit.desc())
        df_products_viewed = df_products_viewed.withColumn('row_number', F.row_number().over(window))
        df_recently_viewed_product = df_products_viewed\
            .filter(F.col('row_number') == 1)\
            .select('UserID', 'ProductID_na_filled') \
            .withColumnRenamed('ProductID_na_filled', 'Recently_Viewed_Product')

        # Compute Pageloads_last_7_days
        cutoff_date = datetime.strptime(self.end_date, '%Y-%m-%d') - timedelta(days=7)
        df = merged_df.filter(
            (merged_df.VisitDateTime_normalized_na_filled >= cutoff_date) &
            (merged_df.Activity_na_filled == 'pageload')
        )
        df_pageloads_last_7_days = df.groupBy('UserID').count().withColumnRenamed('count', 'Pageloads_last_7_days')

        # Compute Clicks_last_7_days
        cutoff_date = datetime.strptime(self.end_date, '%Y-%m-%d') - timedelta(days=7)
        df = merged_df.filter(
            (merged_df.VisitDateTime_normalized_na_filled >= cutoff_date) &
            (merged_df.Activity_na_filled == 'click')
        )
        df_clicks_last_7_days = df.groupBy('UserID').count().withColumnRenamed('count', 'Clicks_last_7_days')

        ## Merge all the computations
        users = self.user_df.select('UserID')
        users = users.join(df_days_visited_7_Days, 'UserID', how='left')

        users = users.join(df_total_products_viewed_15_Days, 'UserID', how='left')

        users = users.join(df_user_vintage, 'UserID', how='left')

        users = users.join(df_most_viewed_product_15_days, 'UserID', how='left')

        users = users.join(df_most_active_os, 'UserID', how='left')

        users = users.join(df_recently_viewed_product, 'UserID', how='left')

        users = users.join(df_pageloads_last_7_days, 'UserID', how='left')

        users = users.join(df_clicks_last_7_days, 'UserID', how='left')

        # Fill nulls
        users = users.na.fill({
            'No_of_days_Visited_7_Days': 0,
            'No_Of_Products_Viewed_15_Days': 0,
            'Most_Viewed_product_15_Days': 'Product101',
            'Recently_Viewed_Product': 'Product101',
            'Pageloads_last_7_days': 0,
            'Clicks_last_7_days': 0
        })
        users = users.sort('UserID')

        return users

    def load(self, df):
        output_path = os.path.join(self.output_dir, 'pipeline_output.csv')
        df.toPandas().to_csv(output_path, index=False)
