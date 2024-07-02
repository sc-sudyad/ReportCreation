class AggregationUtils:
    @staticmethod
    def get_aggregation_interval(aggregation_type):
        intervals = {
            'daily': 'P1D',
            'hourly': 'PT1H',
            'weekly': 'P1W',
            'monthly': 'P1M'
        }
        if aggregation_type in intervals:
            return intervals[aggregation_type]
        else:
            raise ValueError("Unsupported aggregation type")

    @staticmethod
    def get_metric_interval(metric_type):
        intervals = {
            'mean': 'AVG',
            'min': 'MIN',
            'max': 'MAX'
        }
        if metric_type in intervals:
            return intervals[metric_type]
        else:
            raise ValueError("Unsupported metric type")
