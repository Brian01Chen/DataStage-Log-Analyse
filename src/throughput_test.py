import os
import ibm_db
import time
from datetime import datetime

def get_data ():
    try:
        con = ibm_db.connect("DATABASE = DSS;HOSTNAME = trackeruat.rtp.dst.ibm.com;PORT = 50010;\
                              PROTOCOL = TCPIP;\
                              UID = zyibj;PWD = timz1707;","","")

        query_sql = '''
        with sum_bp_deal_id as
        (
            select bp_deal_id, coalesce(sum(Expird_Prratd_To_12_Mths),0) Expird_Prratd_To_12_Mths 
              from DWDM1.maintnc_expird_renwd_fact
             where bp_deal_id is not null
             group by bp_deal_id
        ),
        
        DEAL_SIZE_DIMNSN_ID as
        (
            select 
              bp_deal_id,
              BP_DEAL_SIZE_DIMNSN_ID
            from sum_bp_deal_id join DWDM1.BP_DEAL_SIZE_DIMNSN
              on  Expird_Prratd_To_12_Mths  > bp_deal_size_range_start    
             and  Expird_Prratd_To_12_Mths  <=    bp_deal_size_range_end
        )
        
        select
          final_rec_id, 
          case when f.bp_deal_id is null then 0 
             else bsd.BP_DEAL_SIZE_DIMNSN_ID end 
             as BP_DEAL_SIZE_DIMNSN_ID
        from DWDM1.MAINTNC_EXPIRD_RENWD_FACT_BAK_201707 f inner join DEAL_SIZE_DIMNSN_ID bsd on f.bp_deal_id=bsd.bp_deal_id
        fetch first 5000 rows only
        with ur
        '''
        stmt = ibm_db.exec_immediate(con, query_sql)

        result = ibm_db.fetch_both(stmt)
        while result:
            if os.path.exists (r'c:\DB2'):
                with open('data.csv','w+') as  f:
                    f.write(result)

            result = ibm_db.fetch_both(stmt)
        ibm_db.close(con)
    except Exception as e:
        print (e)
    return result

class Timeit(object):
    def __init__(self, func):
        self._wrapped = func
    def __call__(self, *args, **kwargs):
        start_time = time.time()
        print ("start time is %s" % datetime.now())
        result = self._wrapped(*args, **kwargs)
        print ("end time is %s" % datetime.now())
        print("elapsed time is %s " % (time.time() - start_time))
        print (result)
        return result


if __name__ == '__main__':
    tcal = Timeit(get_data)
    result = tcal.__call__()