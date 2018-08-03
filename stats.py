from elasticsearch import Elasticsearch
import pandas as pd
from tqdm import tqdm
from pprint import pprint

MAX_SIZE = 1000000


def do_query(query_body):
    res = es.search(index="logstat", body=query_body)
    aggs = res['aggregations']['agg_level_1']
    buckets = aggs['buckets']

    print("doc_count_error_upper_bound: %s" % aggs[
        'doc_count_error_upper_bound'])
    print("sum_other_doc_count: %s" % aggs['sum_other_doc_count'])
    print(len(buckets))
    return buckets


def field_exists_query(field):
    q = {
        "bool": {
            "filter": [{
                "exists": {
                    "field": field
                }
            }]
        }
    }
    return q


def terms_query(field, size):
    return {
        "field": field,
        "size": size
    }


def group_by_barcode_query(size=100):
    body = {
        "size": 0,
        "query": field_exists_query("request.body.barcodes.keyword"),
        "aggs": {
            "agg_level_1": {
                "terms": terms_query("request.body.barcodes.keyword", size),
                "aggs": {
                    "agg_level_2": {
                        "terms": terms_query(
                            'response.body.nomenclatures.id.keyword', size)
                    }
                }
            }
        }
    }
    return body


def group_by_user_get_barcode_counts_query(size=100):
    body = {
        "size": 0,
        "query": field_exists_query("request.body.barcodes.keyword"),
        "aggs": {
            "agg_level_1": {
                "terms": terms_query("user.id.keyword", size),
                "aggs": {
                    "agg_level_2": {
                        "cardinality": {
                            "field": "request.body.barcodes.keyword"
                        }
                    }
                }
            }
        }
    }

    return body


def group_by_user_get_search_text_counts_query(size=100):
    body = {
        "size": 0,
        "query": field_exists_query("request.body.search.text.keyword"),
        "aggs": {
            "agg_level_1": {
                "terms": terms_query("user.id.keyword", size),
                "aggs": {
                    "agg_level_2": {
                        "cardinality": {
                            "field": "request.body.search.text.keyword"
                        }
                    }
                }
            }
        }
    }

    return body


def most_frequent_searches_query(size=100):
    body = {
        "size": 0,
        "query": field_exists_query("request.body.search.text.keyword"),
        "aggs": {
            "agg_level_1": {
                "terms": terms_query("request.body.search.text.keyword", size)
            }
        }
    }

    return body


if __name__ == '__main__':
    es = Elasticsearch(timeout=120)

    ##################################################

    body = group_by_barcode_query(size=MAX_SIZE)
    buckets = do_query(body)

    df = []
    for buck in buckets:
        rbuckets = buck['agg_level_2']['buckets']
        found = 0
        for rb in rbuckets:
            found += rb['doc_count']
        df.append({'barcode': buck['key'],
                   'requested': buck['doc_count'],
                   'found': found})

    df = pd.DataFrame.from_records(df)
    df = df[['barcode', 'requested', 'found']]
    df.to_excel('../data/logstash/bcs.xlsx', encoding='utf8', index=False)

    ##################################################

    body1 = group_by_user_get_barcode_counts_query(size=MAX_SIZE)
    body2 = group_by_user_get_search_text_counts_query(size=MAX_SIZE)
    buckets1 = do_query(body1)
    buckets2 = do_query(body2)

    df1 = []
    for buck in buckets1:
        df1.append(
            {'user_id': buck['key'],
             'bc_query_count': buck['doc_count'],
             'bc_unique_query_count': buck['agg_level_2']['value']})

    df2 = []
    for buck in buckets2:
        df2.append(
            {'user_id': buck['key'],
             'search_query_count': buck['doc_count'],
             'search_unique_query_count': buck['agg_level_2']['value']})

    df1 = pd.DataFrame.from_records(df1)
    df2 = pd.DataFrame.from_records(df2)

    merged = df1.merge(df2, on='user_id', how='outer')

    merged = merged[['user_id', 'bc_query_count', 'bc_unique_query_count',
                     'search_query_count', 'search_unique_query_count']]
    merged.to_excel('../data/logstash/user_queries.xlsx', encoding='utf8',
                    index=False)

    ##################################################

    body = most_frequent_searches_query(size=10000)
    buckets = do_query(body)

    df = []
    for buck in buckets:
        df.append({'search_text': buck['key'],
                   'count': buck['doc_count']})
    df = pd.DataFrame.from_records(df)
    df.to_excel('../data/logstash/most_frequent_queries.xlsx', encoding='utf8',
                    index=False)
