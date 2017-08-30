import json
from parse_log_job_hierarchy import *
import os
    
def convert_to_json(message):
    def set_default(obj):
        if isinstance(obj,set):
            return list(obj)
        raise TypeError
    message = json.dumps(message,default=set_default)
    return json.loads(message)
    
def get_job_matrix_json(jobname):
    matrix = parse_ds_job_matrix(jobname)
    dsjob_json = matrix.generate_job_matrix  (jobname)
    #add root nodes
    root_node = {'root':{'nodes':[jobname],'links':[]}}
    dsjob_json.update(root_node)
    return dsjob_json

def get_err_job_matrix_json(jobname):
    matrix = parse_ds_job_matrix(jobname)
    dsjob_json = matrix.generate_err_job_matrix(jobname)
    
    return dsjob_json
   

def get_series_data_normal(dsjob_json):
    '''
    Build Json file Like the hierarchy:
{
    {
    "root": {
        "name": "Batch::SNS_Master_Process",
        "value": {
            "start_time": "2017-07-20 05:47:34",
            "current_time": "2017-07-20 05:53:18",
            "duration": "0:05:44",
            "status": "Finished with warning"
        }
    },
    "Batch::SNS_Master_Process": {
        "nodes": [{
            "name": "Gen_Run_Flag_hash",
            "value": {...}
        }, {
            "name": "Batch::Master_Publish_Start",
            "value": {...}
        },
        ......],
        "links": [{
                "target": "Batch::Master_Publish_Start",
                "source": "Gen_Run_Flag_hash"
            },
            {
                "target": "Batch::Master_Publish_Mail",
                "source": "Gen_Run_Flag_hash"
            },
            ......
        ]
    },
    "Batch::Master_Publish_Start": {
        "nodes": [{
            "name": "Master_Process_Param_Value",
            "value": {...}
        }, {
            "name": "Master_Publish_Param_Value",
            "value": {...}
        }],
        "links": [{
            "target": "Master_Publish_Param_Value",
            "source": "Master_Process_Param_Value"
        }]
    },
    ...
}
    '''
    #find batch nodes if have child node

    batch_nodes = [(batch_node,dsjob_json[batch_node]) for batch_node in dsjob_json
            if 'nodes' in dsjob_json[batch_node]]
    normal_data = {}
    
    for batch_node in batch_nodes:
        parent_node = batch_node[0]
        normal_data[parent_node] = {}
        
        normal_data[parent_node]['nodes'] = []
        child_node = batch_node[1]['nodes']
        for child in child_node:
            child_view = {}
            child_view['name'] = child
            if child in dsjob_json:
                child_info = dsjob_json[child]
                child_view['value'] = child_info['attr']
            normal_data[parent_node]['nodes'].append(child_view)
        
        normal_data[parent_node]['links'] = []
        links = batch_node[1]['links']
        for src in links:
            for tgt in links[src]:
                link = {}.fromkeys(('source','target'))
                link['source'] = src
                link['target'] = tgt
                link_copy = link.copy()
                normal_data[parent_node]['links'].append(link_copy)
    return normal_data

def get_series_data_err(dsjob_json):
    err_data = {}
    err_data['nodes'] = []
    err_data['links'] = []
    
    node_data = dsjob_json['nodes']
    for node in node_data:
        node_value = {}
        node_value['name'] = node
        node_value['value'] = node_data[node]
        node_value_copy = node_value.copy()
        err_data['nodes'].append(node_value_copy)
        
    link_data = dsjob_json['links']
    for src in link_data:
        for tgt in link_data[src]:
            link = {}.fromkeys(('source','target'))
            link['source'] = src
            link['target'] = tgt
            if src in node_data and tgt in node_data :
                
                link_copy = link.copy()
                err_data['links'].append(link_copy)
    
    return err_data

def export_to_echars_json(base,jobname):
    
    dsjob_json = get_job_matrix_json(jobname)
    dsjob_err_json = get_err_job_matrix_json(jobname)
    echars_data = {}.fromkeys(('normal_data','err_nodes','err_links'))
    
    dsjob_json = get_job_matrix_json(jobname)
    dsjob_err_json = get_err_job_matrix_json(jobname)
    normal_data = get_series_data_normal(dsjob_json)
    err_data = get_series_data_err(dsjob_err_json)
    echars_data['normal_data'] = normal_data
    if err_data != {}:
        echars_data['err_nodes'] = err_data['nodes']
        echars_data['err_links'] = err_data['links']
    print (echars_data)
    def set_default(obj):
        if isinstance(obj,set):
            return list(obj)
        raise TypeError
    if base == '.' or base == '':
        base = os.getcwd()
    if os.path.exists(base):
        filename = '%s\%s' %(base,'job.json')
        print (filename)
        with open(filename,'w') as f:
            f.write(json.dumps(echars_data,default=set_default))
    
if __name__ == '__main__':
    '''
    Batch::RMS_Process;Batch::Load_Entmt_Sum_Fact_Nightly;
    Batch::Load_Entmt_Sum_Fact_Incr
    Batch::Rmt_Master_Load_Sequence_Adhoc
    Batch::Load_Dimnsns
    '''
    root = 'Populate_Token_Multi_Yr_Sns_Part'
    base = r'C:\Users\IBM_ADMIN\AppData\Local\Programs\Python\Python35\Scripts\RelateJobs\RelateJobs'
    export_to_echars_json(base,root)
    
