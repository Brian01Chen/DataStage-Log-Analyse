from influxdb import InfluxDBClient


def conn_influxdb(host_ifx,username,password,db):
    ifx = InfluxDBClient(host=host_ifx,port='8086',
                         username = username,
                         password = password,
                         database = db)
    return ifx

def write_data_flow(ifx,info_dict):
    try:
        inflx_body = []
        metrics = {}
        metrics['measurement'] = "datastage_summary"
        tags = {}
        tagkeys = ['jobname','project','status','current_job']
        tags = dict((k,v) for k,v in info_dict.items() if k in tagkeys)
        metrics['tags'] = tags
        valuekeys = info_dict.keys()-tagkeys

        #detail message would not show on influxdb.
        valuekeys.remove('error_info')
        fields = {}
        fields = dict((k,v) for k,v in info_dict.items() if k in valuekeys)
        metrics['fields'] = fields
        #each record should be json type.
        inflx_body.append(metrics)
        ifx.write_points(inflx_body)
    except Exception as e:
        print (e)

#ifx = conn_influxdb('9.111.121.141','root','.lo98ik,','dsdb')
