'''
Created on Dec 15, 2016

@author: manthk
'''

import os
import re
import psycopg2
from astropy.units import das

ambari = '10.111.156.210'
isilon = 'hdfs://10.111.158.181:8020/keith/cdhfed/hdfs/'
das = 'hdfs://10.111.156.210:8020/user/'
 
def addFederation():
    conn_string = "host='"+ambari+"' dbname='ambari' user='ambari' password='bigdata'"
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(" select version_tag from clusterconfigmapping where type_name = 'core-site' and selected = 1")
    verResult = cursor.fetchone()
    
    cursor.execute("select * from clusterconfig where type_name = 'core-site' and version_tag = '"+verResult[0]+"'")
    result = cursor.fetchone()
    print("ConfigID: %s  config data: %s" % (result[0], result[6]))
    
    curConf = result[6]
    
    newConf = curConf[:-1]
    newConf += ',"fs.viewfs.mounttable.cdhfed.homedir" : "/","fs.viewfs.mounttable.cdhfed.link./DAS":"'+das+'","fs.viewfs.mounttable.cdhfed.link./Isilon":"'+isilon+'"}'
    
    
    print("Fixed String = %s" % (newConf))
    
    sqlConfigChg = "update clusterconfig set config_data = '"+newConf+"' where type_name = 'core-site' and version_tag = '"+verResult[0]+"'"
    
    print("SQL String = %s" % (sqlConfigChg))
    
    cursor.execute(sqlConfigChg)
    
    cursor.execute("select * from clusterconfig where type_name = 'core-site' and version_tag = '"+verResult[0]+"'")
    result = cursor.fetchone()
    print("ConfigID: %s  config data: %s" % (result[0], result[6]))
    
addFederation()


