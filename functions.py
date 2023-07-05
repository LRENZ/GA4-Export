import os
from dataclasses import dataclass
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    Filter,
    FilterExpression,
)
import json


@dataclass
class Properties:
    with open('../properties.json') as f:
        data = json.load(f)


@dataclass
class Views:
    # Datatype used in GA. This var is used in the for loop below.
    default_type = 'STRING'

    # List of desired dimensions from GA. name for date must be 'date' for partitioning to work.
    # limit of 7 dimensions here and if using a segment then segment must be included as a dimension.
    dimension_names = [{"name": "date"},
                       {"name": "pageTitle"},
                       {"name": "city"},
                       {"name": "region"},
                       {"name": "sessionSourceMedium"},
                       {"name": "pagePath"}]

    # final list that will contain dimension name and datatype after going through the for loop below.
    dimensions = []

    # creates list of dictionaries that will be used to upload to BigQuery
    for name in dimension_names:
        if name['name'] != 'date':
            dimensions.append({'name': name['name'], 'type': default_type})
        else:
            dimensions.append({'name': name['name'], 'type': 'DATE'})

    metrics = [{"name": "screenPageViews", 'type': "INTEGER"}, ]

    # WILL PROVIDE DATA UP UNTIL AND INCLUDING END DATE. The inclusive end date for the query in the format
    # YYYY-MM-DD. Cannot be before startDate. The format NdaysAgo, yesterday, or today is also accepted, and in that
    # case, the date is inferred based on the property's reporting time zone.
    end_date = 'yesterday'
    start_date = '10daysAgo'

    # will append to bigquery if true else will OVERWRITE table.
    append = False

    # if left on false segmentId won't be considered
    segmentsBool = False
    segmentId = 'Em53XkOgTMC4Y1YAP4DwRQ'

    # if left on false filters won't be considered
    filtersBool = False
    filters = 'ga:transactionId=@|BP|CP|'

    # dataset in biquery
    dataset_id = 'ga4'
    table_name = 'views'


def parse_data(res, query):
    """
    takes special output datatype from GA4 and converts to standard dictionary
    :param res: object returned from GA4 api
    :param query: table/class with predefined metrics and dimensions
    :return:
    """
    data = {}
    for x, dim_dict in enumerate(query.dimensions):
        data[dim_dict['name']] = []

    for x, met_dict in enumerate(query.metrics):
        data[met_dict['name']] = []
    for row in res.rows:
        for x, dim_dict in enumerate(query.dimensions):
            data[dim_dict['name']].append(row.dimension_values[x].value)

        for x, met_dict in enumerate(query.metrics):
            data[met_dict['name']].append(row.metric_values[x].value)

    return data

# TODO: make user set default property id
def sample_run_report(property_id="323042075", query=Views, offset=0):
    """Runs a simple report on a Google Analytics 4 property.

    The API returns a maximum of 100,000 rows per
    request, no matter how many you ask for. ``limit`` must be
    positive.

    """
    client = BetaAnalyticsDataClient.from_service_account_json(os.path.abspath('../gaauth.json'))

    if query.filtersBool:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=x['name']) for x in query.dimensions],
            metrics=[Metric(name=x['name']) for x in query.metrics],
            date_ranges=[DateRange(start_date=query.start_date, end_date=query.end_date)],
            limit=100000,
            offset=offset,
            return_property_quota=True,
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name=query.filters_field,
                    string_filter=Filter.StringFilter(value=query.filters_value, match_type=6),
                )
            ),


        )
    else:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=x['name']) for x in query.dimensions],
            metrics=[Metric(name=x['name']) for x in query.metrics],
            date_ranges=[DateRange(start_date=query.start_date, end_date=query.end_date)],
            limit=100000,
            offset=offset,
            return_property_quota=True,
        )
    response = client.run_report(request)

    return response