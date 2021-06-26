


class MarketingModelETLPipeline:
    def __init__(self, spark_session, user_df, visitor_logs_df, start_date, end_date, output_path):
        self.spark_session = spark_session
        self.user_df = user_df
        self.visitor_logs_df = visitor_logs_df
        self.start_date = start_date
        self.end_date = end_date
        self.output_path = output_path

    def load(self, df):
        df.write.csv(self.output_path)

    def _filter_visitor_logs(self):
        pass

    def run(self):
        self.user_df.show()
        self.visitor_logs_df.show()
