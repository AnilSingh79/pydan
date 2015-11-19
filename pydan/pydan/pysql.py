'''
Created on Oct 6, 2015

@author: singan22
'''

import sys
from pydan.pytools import print_error


def create_table_qry(tabName, varNames, varTypes=[], uniqueIdFlag=False,uIdName='PYDAN_ROW_NUM'):
    try:
        query = ' CREATE  TABLE '+tabName+''' ( \n'''
        if(uniqueIdFlag == True):
            query = query + uIdName+'        VARCHAR,\n '
        varterms = []
       
        if (len(varTypes)==0):
            varTypes = ['VARCHAR']*len(varNames)
           
        for varname, vartype in zip(varNames,varTypes):
            
            varterms.append('       '.join([varname,vartype]))
            
            
             
        query = query+ ',\n '.join(varterms)
        query = query+'\n )'
     
        return query
    except Exception as err:
        print_error(err, 'pysql.create_table_qry')
        
def insert_qry(tabName,valList,uniqueId=0):
    query = '''INSERT INTO '''+tabName+''' VALUES ('''+str(uniqueId)+', '
    query = query + ','.join('"{0}"'.format(v) for v in valList)
    query = query +')\n'
    return query

def select_data_qry(varNames,srcNames,conditions=['1=1'],groupby=[],orderby=[],limit=-1,offset=-1):
    ##Create a dictionary to hold the results.
    #print varNames
    #print srcNames
    ##print groupby
    if len(varNames)==0:
        raise RuntimeError("ERROR: Specify at-least one variable")
    if len(srcNames)==0:
        raise RuntimeError("ERROR: Specify at-least one source table")
    try:
        query = '\nSELECT\n  '
        query = query + ',\n '.join(varNames)+'\nFROM\n '+',\n '.join(srcNames)
        query = query+'\nWHERE\n '
        query = query + '\n and '.join(conditions)
        if(len(groupby)>0):
            query = query + "\nGROUP BY "
            query = query + ",\n".join(groupby)
        if(len(orderby)>0):
            query = query + "\nORDER BY "
            query = query + ",\n".join(orderby)
        if limit>0 :
            query = query+"\nLIMIT "+str(limit)
        if offset > 0:
            query = query + '\nOFFSET '+str(offset)
            
        return query
    except Exception, err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        return ''

    

def create_view_qry( viewName, varNames,srcNames, conditions=[],groupby=[],orderby=[]):
    selQry =  select_data_qry(varNames, srcNames, conditions, groupby, orderby)
    query = ('CREATE VIEW '+viewName+' AS \n' )+selQry
    return query
    
   

def update_column_qry(tabName,colName,oVal='old',nVal='new'):
    query = 'UPDATE '+tabName+'\n'
    query = query+'SET '+colName+" = '"+nVal+"'\n"
    query = query+'WHERE '+colName+" = '"+oVal+"'\n"
    return query
      
      
def get_column_metrics_qry(tabName,varName,conditions=['1=1'],groupby=[],orderby=[]):
    
    varNames = ["'"+varName+"' as varName "]
    varNames.append('COUNT('+varName+') as count')
    varNames.append('SUM('+varName+') as mean')
    varNames.append('MIN('+varName+') as min')
    varNames.append('MAX('+varName+') as max')
    varNames.append('SUM('+varName+'*'+varName+') as sumsquare')
    query = select_data_qry(varNames,[tabName])
    return query

def get_column_err2_qry(tabName,varName,mean,conditions=['1=1'],groupby=[],orderby=[]):
    strMean  = str(mean)
    varNames = ["'"+varName+"' as varName "]
    varNames.append('SUM((('+varName+')-('+strMean+'))*(('+varName+')-('+strMean+'))) as err2')
    query = select_data_qry(varNames,[tabName])
    return query

    
    
def get_median_qry(tabName,varName,count,conditions=['1=1'],groupby=[],orderby=[]):
    ##Now let us find the median.
    c = int(count);
    limit = str(2 - (c)%2)
    offset = str ((c-1)/2)
    query = select_data_qry([varName], [tabName], orderby=[varName])
    query = query+'\nLIMIT '+str(limit)
    query = query+'\nOFFSET '+str(offset)    
    query = select_data_qry(['AVG('+str(varName)+')'],["("+str(query)+")"])
    
    return query
    
def get_binning_query(tabName,lowBinDict,hiBinDict,conditions=['1=1'],orderby=[],limit=-1,offset=-1):
    try:
        qryVars = []
        varNames = lowBinDict.keys()
        groupby = lowBinDict.keys()
        for varName in varNames:
            lBins = lowBinDict[varName]
            hBins = hiBinDict[varName]
            qryVars.append(get_binning_casequery(tabName, varName, lBins, hBins))
        query = select_data_qry(qryVars,[tabName], conditions = conditions, limit= limit, offset = offset)
        ##varNames.append('COUNT(*)')
        ##query = select_data_qry(varNames, ['('+query+')'], conditions = conditions,groupby=groupby)
        ##print query
        return query
    except Exception as err:
        print_error(err,'pysql.get_binning_query')
        

def drop_table_qry(tabName, tabType=' VIEW '):
    try:
        return 'DROP '+tabType+' IF EXISTS '+tabName
    except Exception as err:
        print_error(err, 'drop_table_qry')


            
def get_binning_casequery(tabName,varName,lBinEdges,hBinEdges):
    try:
        firstBin = str(lBinEdges[0])
        case_query = "CASE \n WHEN ("+varName+' < '+firstBin+') THEN ('+firstBin+'-0.001)\n'
        
        ##ADD VALUES TO TAKE CARE OF UNDERFLOW-OVERFLOW... may by call them lowest-0.01 and highest+0.01
        for lBin,hBin in zip(lBinEdges,hBinEdges):
            phrase = 'WHEN ('+varName+' >= '+str(lBin)+' AND '+varName+' < '+str(hBin)+') THEN '+ str(lBin)+' \n '
            case_query = case_query+phrase
        
        case_query += 'WHEN ('+varName+' > '+str(hBinEdges[len(hBinEdges)-1])+') THEN '+str(hBinEdges[len(hBinEdges)-1])+'\n'
        case_query += 'END '+varName+'\n'       
        return case_query
    except Exception as err:
        print_error(err, 'pydataview.get_binning_casequery')
    






