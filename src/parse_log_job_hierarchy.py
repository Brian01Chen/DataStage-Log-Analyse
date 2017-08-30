from parse_log_job_detail import *
from parse_log_es import *



HOST_PORT = [{'host':'9.111.139.77','port':9200}]

def get_es_job_info(jobname):


    es = conn_elasticsearch(HOST_PORT)
    jobinfo = list(search_by_job(es,jobname))
    result = {}.fromkeys(('attr','hierarchy','err'),{})
    if len(jobinfo)>0:
        ps = parse_dsjob_detail(jobinfo)
        
        job_info_dict = ps.get_job_summary_runinfo()
        focus_keys = ['status','start_time','current_time','duration']
        result['attr']= dict([(key,job_info_dict[key]) for key in job_info_dict if key in focus_keys])
        
        hierarchy = ps.get_job_sequence()
        result['hierarchy'] = hierarchy

        err_msg = ps.get_error_job_msg()
        result['err'] = err_msg
        
    return result

class parse_ds_job_matrix(object):
    def __init__(self,jobname):
        self.jobname = jobname
        
    def generate_job_matrix(self,jobname):
        
        edges = {}
        
        def generate_matrix(jobname):
                
            data = get_es_job_info(jobname)
            if data != {}:
                edges[jobname] = {}
                edges[jobname]['attr'] = data['attr']
                
                hierarchy = data['hierarchy']

                if len(list(hierarchy)) > 0:
                    
                    links = hierarchy
                    nodes = list(hierarchy.keys())
                    edges[jobname]['nodes'] = nodes
                    edges[jobname]['links'] = links
 
                    for job in nodes:
                        generate_matrix(job)
          
        generate_matrix(jobname)
        return edges


    def generate_err_job_matrix(self,jobname):
        edges = {}
        edges['links'] = {}
        edges['nodes'] = {}
        
        def generate_matrix(jobname):

            data = get_es_job_info(jobname)
            if data['err'] != {}:
                err_data = data['err']
                values = {}
                values['err_msg'] = err_data[jobname]
                values.update(data['attr'])
                edges['nodes'][jobname] = values
                err_jobs = {err_job for err_job in err_data.keys() if err_job != jobname}
                edges['links'][jobname] = err_jobs
                if len(list(err_jobs)) > 0 :
                    for job in err_jobs:
                        generate_matrix(job)
                                    
        generate_matrix(jobname)
        return edges
    
    


if __name__ == '__main__':
    
    '''Batch::Load_Evoltn_Dtl_Fact_Incr,Batch::Load_Entmt_Sum_Fact_Nightly,Batch::RMS_Process'''
    job = 'Batch::SNS_DTL_SUM_Fact'
    ps = parse_ds_job_matrix(job)
        
    matirx = ps.generate_job_matrix(job)
    print (matirx)
    err_matirx = ps.generate_err_job_matrix(job)
    print (err_matirx)
        
        
    
            
            
