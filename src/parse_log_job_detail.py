import regex as re
from datetime import *
from functools import reduce
from parse_log_es import *
  
CHECK_REGEX_Status = re.compile(r"""FINISHED|RESET""",re.I|re.X)  
CHECK_REGEX_Jobname = re.compile(r"""
             (?<=(Starting\sJob\s+))\w+[.]?(::)?\w+  |
             (?<=(->\s+\()).*?(?=\))                 |
             (?<=(<-\s+))\w+[.]?(::)?\w+   
        """,re.I|re.X)
CHECK_REGEX_DSSTASTUS = re.compile(r"""(?<=(\()).*?(?=(\)))""",re.I|re.X)

def throw_exception():
    raise Exception('Input Jobinfo Null!')
def remove_duplicate_element(job_list):
    func = lambda x,y : x if y in x else x+[y]
    return reduce(func,[[],]+job_list)
        
class parse_dsjob_detail(object):
    def __init__(self,jobinfo):

        #jobinfo can not be null or []
        if len(jobinfo) == 0 :
            throw_exception()
        #sort jobinfo data by eid
        jobinfo = sorted(jobinfo,key = lambda x: x['eid'])

        self.jobinfo = jobinfo
        self.jobname = jobinfo[0]['job']
        self.job_list = []
        self.error_list = []

        self.job_status = self.get_job_status() if self.get_job_status() else 'Start'
        self.project = jobinfo[0]['project']
        self.start_eid = int(jobinfo[0]['eid'])
        self.end_eid = int(jobinfo[-1]['eid'])
        self.start_time = jobinfo[0]['@timestamp']

        self.end_time = jobinfo[-1]['@timestamp']


    def get_job_status(self):
        
        error_status = {info['dstype'] for info in self.jobinfo if info['dstype'] in ('WARNING','FATAL')}
        run_status = {CHECK_REGEX_Status.search(info['logmessage']) for info in self.jobinfo if info['dstype'] in ('STARTED','RESET')}
        if None in run_status:
            run_status.remove(None)
        if error_status | run_status == set():
            job_status = 'Running'
        elif len(error_status) == 0:
            job_status = 'Finished'
        elif len(error_status) > 0 and len(run_status) == 0:
            job_status = 'FATAL 'if 'FATAL' in run_status else 'WARNING'
        else:
            job_status = 'Finished with warning'
        return job_status

    
    def get_all_jobs(self):

        current_job_queue = [[int(info['eid']),CHECK_REGEX_Jobname.search(info['logmessage']).group()]
                             for info in self.jobinfo if info['dstype'] in ('BATCH','STARTED')
                             and CHECK_REGEX_Jobname.search(info['logmessage'])]
        job_list = [job for job in current_job_queue if job[1] is not None]
        return job_list
        
    def get_current_job(self):
        job_list = self.get_all_jobs()
        current_job = job_list[-1][1]
        return current_job
                
    def get_error_job_msg(self):

        job_list = self.get_all_jobs()
        error_list = [[int(info['eid']),info['logmessage']]
                      for info in self.jobinfo if info['dstype'] in ('WARNING','FATAL') ]
        #print (error_list)
        '''find out the err job name

           'eid':100,'job':'Batch::BRAND_DIMNSN','type':'Batch',
                'logmessage':'Batch::BRAND_DIMNSN -> (BRAND_DIMNSN): Job run requested...'
           ......
           'eid':110,'job':'Batch::BRAND_DIMNSN','type':'WARNING','logmessage':'...not finished...'
           'eid':120,'job':'Batch::BRAND_DIMNSN','type':'Batch',
                'logmessage':''Batch::BRAND_DIMNSN -> (Recon_Done_Flag): Job run requested...''

           if try to find the err jobname on the second row with eid = 110, use the solution compare eid.
        '''
        for err_eid in error_list:
            for job_eid in job_list[::-1]:
                #check the err job use eid to compare
                if err_eid[0] > job_eid[0]:
                    err_eid.append(job_eid[1])
                    break
        err_msg = {}
        err_jobs = list({job[2] for job in error_list if len(job) == 3})


        '''merge job err_msg with the same jobname.
           case:
           error_list:
               [['BRAND_DIMNSN','duplicate rows when ...'],
                ['BRAND_DIMNSN','BRAND_DIMNSN did not finish OK ...']
           so different msg need merge when with the same name,
           final result:
              {'BRAND_DIMNSN',['duplicate rows when ...','BRAND_DIMNSN did not finish OK ...']}              
        '''
        for job in err_jobs:
            job_err_msg = []
            for err in error_list:
                if len(err) == 3:
                    if job == err[2]:
                        job_err_msg.append(err[1])
            err_msg[job] = job_err_msg
        
        if self.jobname not in err_msg and len(err_jobs) > 0:
            status = self.get_job_status()
            #err_msg data type always keey list.
            err_msg[self.jobname] = [self.jobname + ' ' + status]
        return err_msg
    
    
    def get_job_sequence(self):
        batch_job_info_queue = [(num,CHECK_REGEX_Jobname.search(info['logmessage']).group()) for num,info in enumerate(self.jobinfo)
                        if info['dstype'] == 'BATCH' and CHECK_REGEX_Jobname.search(info['logmessage'])]
        '''
           1)check the previous job status is Runing or Complete. use the index - 1.
           2)only focus on the children jobs,exclude the parent job.
        '''
        job_ind_name_list = [(job[0]-1,job[1]) for job in batch_job_info_queue if job[1]!= self.jobname ]

        job_sequence = {}
        
        if len(job_ind_name_list) == 0:
            return job_sequence
        
        
        for job in job_ind_name_list:
            #current index in job list
            index = job_ind_name_list.index(job)
            if index == 0 :
                if index+1 <= len(job_ind_name_list) - 1:
                    job_sequence[job[1]] = {job_ind_name_list[index+1][1]}
                else:
                    job_sequence[job[1]] = set()
            dsstatus = CHECK_REGEX_DSSTASTUS.search(self.jobinfo[job[0]]['logmessage'])

            if dsstatus :
                if dsstatus.group() in ('DSRunJob','DSWaitForJob'): 
                    job_sequence[job_ind_name_list[index-2][1]] = {job_ind_name_list[index-1][1],job[1]}
                    if index+1 <= len(job_ind_name_list) - 1 :
                        job_sequence[job[1]] = {job_ind_name_list[index+1][1]}
                        job_sequence[job_ind_name_list[index-1][1]] = {job_ind_name_list[index+1][1]}
                    else:
                        job_sequence[job[1]] = set()
                        job_sequence[job_ind_name_list[index-1][1]] = set()
                else:
                    if index+1 <= len(job_ind_name_list) - 1:
                        job_sequence[job[1]] = {job_ind_name_list[index+1][1]}
                    else:
                        job_sequence[job[1]] = set()

        return job_sequence

    
        
    def get_job_summary_runinfo(self):
    
        header = ('jobname','project','status','current_job','start_eid','start_time','current_eid','current_time','duration','error_info')
        info_dict = {}.fromkeys(header)
        info_dict['jobname'] = self.jobname
        info_dict['project'] = self.project
        info_dict['current_job'] = self.get_current_job()
        info_dict['status'] = self.job_status
        info_dict['start_eid'] = self.start_eid
        
        err_msg = self.get_error_job_msg()
        if err_msg != {}:
            if self.jobname in err_msg:
                err_msg = err_msg[self.jobname]
        info_dict['error_info'] = err_msg
        
        t1 = re.sub('T|[.]\d+Z',' ',self.start_time).strip() 
        info_dict['start_time'] = t1
        
        info_dict['current_eid'] = self.end_eid
        t2 = re.sub('T|[.]\d+Z',' ',self.end_time).strip() 
        info_dict['current_time'] = t2
        try:
            info_dict['duration'] = str(datetime.strptime(info_dict['current_time'],'%Y-%m-%d %H:%M:%S') - \
                                datetime.strptime(info_dict['start_time'],'%Y-%m-%d %H:%M:%S'))
        except:
            print ('error time')
            pass

        return info_dict
    
if __name__ == '__main__':

    def get_es_job_info(jobname):
        host_port = [{'host':'9.111.139.77','port':9200}]
        es = conn_elasticsearch(host_port)
        return search_by_job(es,jobname)
    jobinfo = list(get_es_job_info('Batch::Rmt_Master_Load_Sequence_Adhoc'))
    print (jobinfo)
    ps = parse_dsjob_detail(jobinfo)
    #sequence = ps.get_job_sequence()
    #print (sequence)
    #errms = ps.get_error_job_msg()
    #print (errms)
    print (ps.job_status)
