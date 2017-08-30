from elasticsearch import Elasticsearch
import regex as re
from datetime import *
import time as timedtl
from parse_log_es_index_template import *
from parse_log_job_detail import *

INDEX_PERIOD_WINDOW = 1000

def conn_elasticsearch(hosts,
            sniff_on_start=True,
            sniff_on_connection_fail=True,
            sniffer_timeout=600):

    es = Elasticsearch(hosts)
    return es

def get_all_index(es):

    #get all index on elasticsearch
    all_index = es.cat.indices(h=['index'])
    time_wind = timedelta(days = INDEX_PERIOD_WINDOW)
    ds_ind_list = [ind for ind in all_index.split() if ind.startswith('datastage_run')
                   and datetime.strptime(ind.split('-')[1],'%Y.%m.%d') >
                   datetime.now() - time_wind
                  ]
    #sorted the index list by desc
    ds_ind_list = sorted(ds_ind_list,reverse=True)
    return ds_ind_list

def search_by_job(es,jobname):

    #get the first eid which identify the job start record.
    dsl_job_start = {
            "query":{
                "bool":{
                   "must":[
                       {"term":{"job":jobname}}
                    ],
                   "filter":[
                       {"term":{"logmessage":"starting"}},
                       {"term":{"dstype":"STARTED"}},
                       {"term":{"_type":"logsum"}}
                    ]
                }
            },
            "aggs" : {
                "max_eid": {"max":{"field":"eid"}},
                "max_time":{"max":{"field":"@timestamp"}}
            }
    }
    

    #set size=0, only get aggregation max eid and start time.
    start_log = es.search(body = dsl_job_start,size = 0)
    start_id = start_log['aggregations']['max_eid']['value']

    start_time = start_log['aggregations']['max_time']['value']
    if type(start_time) ==  float:
        start_time = timedtl.strftime('%Y-%m-%dT%H:%M:%SZ', timedtl.gmtime(start_time/1000))

    dslbody = {
            "query":{
                "bool":{
                   "must":[
                      {"term":{"job":jobname}}
                      
                    ],
                   "filter":[
                       {"range":{"eid":{"gte":start_id}}},
                       {"range":{"@timestamp":{"gte":start_time}}},
                       {"term":{"_type":"logsum"}}]
              }
            }
    }
    
    #get the job's last run info
    log = es.search(body = dslbody,
                    _source_exclude = ['tags','@version','time'],#tags,@version,time not display
                    size = 100)

    for log in log['hits']['hits']:
        yield log['_source']


def fetch_job_log_list(joblist,es):

    all_jobinfo = {}
    for job in joblist:
        jobinfo = []

        jobinfo = list(search_by_job(es,job))
        all_jobinfo[job] = jobinfo
    return all_jobinfo

def create_statics_index(es,all_jobinfo_dict):

    title = 'datastage-summary-run-'
    cur_day = str(date.today())
    index_name = title + cur_day
    type_name = 'log_statics'

    bulk_data = []
    for job_dict in all_jobinfo_dict:
        dsjobid = job_dict['jobname']+cur_day
        data_dict = job_dict
        bulk_data.append({
            "index":{
                "_index": index_name,
                "_type": type_name,
                "_id": dsjobid
            }
        })

        bulk_data.append(data_dict)

    if es.indices.exists(index_name):
        print ("deleting '%s' index..." % (index_name))
        res = es.indices.delete(index = index_name)
        print(" response: '%s'" % (res))
    # since we are running locally, use one shard and no replicas
    request_body = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    print("creating '%s' index..." % (index_name))
    res = es.indices.create(index = index_name, body = request_body)
    print(" response: '%s'" % (res))


    #create template for datastage statics 
    if es.indices.exists_template(name = 'datastage-statics-template') == False:
        es.indices.put_template(name = 'datastage-statics-template',
                                body = DS_STATICS_TEMPLATE)


    # bulk the new index job's statics data
    print("bulk indexing...")
    res = es.bulk(index = index_name, body = bulk_data, refresh = True)
    query = es.search(index = index_name)
    print ("query result : '%s'" % query)

if __name__ == '__main__':
    host_port = [{'host':'9.111.139.77','port':9200}]
    es = conn_elasticsearch(host_port)

    joblist = ['Batch::Load_Entmt_Sum_Fact_Incr']
    all_jobinfo = fetch_job_log_list(joblist,es)
    print (all_jobinfo)

    
