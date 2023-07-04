import time
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import numpy as np
import sys
from functions import *
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)


def main(query, property_name):
    """
    takes a query/table and asks GA4 api for data, uploads data to BigQuery
    :param query: table dimensions and metrics along with other parameters that define how table is uploaded to BQ
    """

    property_id = Properties.data[property_name]['property_id']
    dataset = Properties.data[property_name]['dataset']
    bigquery_project = input('please input your bigquery project ID: ')

    res = sample_run_report(query=query, property_id=property_id)

    print('API limits:')
    print(res.property_quota, '\n')

    data = parse_data(res, query)
    df = pd.DataFrame(data)

    # append to dataframe until we aren't limited by 100,000 api row limit
    counter = 1
    while len(df) % 100000 == 0:
        print('request counter:')
        print(counter, '\n')

        res = sample_run_report(query=query, offset=100000 * counter, property_id=property_id)
        data = parse_data(res, query)
        df1 = pd.DataFrame(data)
        df = pd.concat([df, df1])

        print("dataframe length:")
        print(len(df), '\n')

        if len(df) == 0:
            raise Exception('query isn\'t returning new rows')

        time.sleep(1 + np.random.random())
        counter += 1

    for metric in query.metrics:
        if metric['type'] == 'INTEGER':
            df[metric['name']] = df[metric['name']].apply(lambda x: int(x))
        elif metric['type'] == 'FLOAT':
            df[metric['name']] = df[metric['name']].apply(lambda x: float(x))

    print('size of dataframe:')
    print(str(np.round(sys.getsizeof(df) / 1000000)) + ' MB', '\n')

    df['date'] = pd.to_datetime(df['date'])

    try:
        df.columns = query.column_names
        column_names_specified = True
    except AttributeError:
        column_names_specified = False
        print('no column names specified')

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath('gcpauth.json')

    client = bigquery.Client()

    def data_type(datatype):
        if datatype == 'STRING':
            return bigquery.enums.SqlTypeNames.STRING
        elif datatype == 'INTEGER':
            return bigquery.enums.SqlTypeNames.INTEGER
        elif datatype == 'DATE':
            return bigquery.enums.SqlTypeNames.DATE
        elif datatype == 'FLOAT':
            return bigquery.enums.SqlTypeNames.FLOAT

    values = [bigquery.SchemaField(x['name'], data_type(x['type'])) for x in query.dimensions] + [
        bigquery.SchemaField(x['name'], data_type(x['type'])) for x in query.metrics]

    if column_names_specified:
        values = []
        all_columns = query.dimensions + query.metrics

        for x, column in enumerate(all_columns):
            values.append(bigquery.SchemaField(query.column_names[x], data_type(column['type'])))

    job_config = bigquery.LoadJobConfig(
        schema=values,
        autodetect=True,
    )

    if query.append is True:
        job_config.write_disposition = 'WRITE_APPEND'
    else:
        job_config.write_disposition = 'WRITE_TRUNCATE'

    job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="date",)

    # -----------uncomment for debugging-----------
    # df_without_index = df.reset_index(drop=True)
    # df.to_csv('debug1.csv')
    # df.index.name = 'index'
    # Check if the dataset exists
    try:
        GCP_change_dataset_result = client.get_dataset('bello')
    except NotFound:
        print('dataset not found, creating dataset....')
        GCP_change_dataset_result = bigquery.Dataset(bigquery_project + '.' + 'bello')
        GCP_change_dataset_result = client.create_dataset(dataset)

    print(df, '\n')
    print(job_config.schema)

    print('{}.{}.{}'.format(bigquery_project, dataset, query.table_name))
    job = client.load_table_from_dataframe(df, '{}.{}.{}'.format(bigquery_project, dataset, query.table_name),
                                           job_config=job_config)
    print(job.result())

    table = client.get_table('{}.{}.{}'.format(bigquery_project, dataset, query.table_name))

    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema),
            '{}.{}.{}'.format(bigquery_project, dataset, query.table_name)))


if __name__ == '__main__':
    queries = []

    classes_called = sys.argv[1:]
    for aclass in classes_called:
        queries.append(globals().get(aclass))

    drop_nones_queries = [x for x in queries if x is not None]
    queries = drop_nones_queries

    for property_name, value in Properties.data.items():
        if property_name in sys.argv:
            for query in queries:
                main(query, property_name)