'''
Created on Oct 7, 2015

@author: Anil Singh, for Daddy's Ersa :)

Major Update : 4/8/2016
'''

import sqlite3
import math
import re
import numpy as np
import datetime as dt

from pytools import print_error
from pytools import clean_text
from pytools import clean_string
from pytools import *

from pysql  import *
from pypaint.pyhist import TH1D
from pypaint.pygraph import TGraph
import matplotlib.dates as mdate




class pydset(object):
    def __init__(self,dbaseName,srcName, srcType=' TABLE ',conn=None):
        '''
        A pydset instance is initialized over an existing table in SQLITE file.
        '''
        try:
            self.dbaseName = dbaseName;
            if conn == None:
                self.pConn = sqlite3.connect(dbaseName)
            else: self.pConn = conn
            self.srcName = srcName.strip()
            self.srcType = srcType.strip()
            self.colNames = []
            self.colTypes = {}
            self.persist = True
            query = 'PRAGMA table_info('+self.srcName+')'
            colDetails = self.pConn.cursor().execute(query)
            numrows = 0
            for colDetail in colDetails:
                colName,g = clean_text(colDetail[1])
                colType = clean_string(colDetail[2])
                self.colNames.append(colName) 
                self.colTypes[colName]=colType
                numrows = numrows+1
            if numrows == 0:
                raise ValueError("ERROR: Can't find table "
                                 +srcName+" in database "+dbaseName)
        except Exception as err:
            print_error(err,'pydset.__init__')
    
    def getColNames(self):
        return [col for col in self.colNames]
    
    def __del__(self):
        if self.persist == False:
            self.drop()
        ####self.drop()   
        self.srcName=None
        self.srcType= None
        self.colNames = None
        self.colTypes = None

    def drop(self):
        '''
        Drop the underlying table from database.
        '''
        try:
            query = drop_table_qry(tabName=self.srcName,tabType=self.srcType)
            #print (query)
            self.pConn.execute(query)
            self.pConn.commit()
        except Exception as err:
            print_error(err,'pydset.drop')
                        
    def union(self, guy, resultTab, resultType=' TABLE '):
        '''
        Append two pydset objects. The regular SQL UNION rules apply:
          1. Both objects have same number of columns.
          2. Corresponding columns must have same name.
          3. Corresponding columns must have same type.
        ''' 
        try:
            self.pConn.execute(drop_table_qry(resultTab,resultType))
            self.pConn.commit()
            allVars1 = [var for var in self.colNames]
            
            query = 'CREATE ' + resultType +'  '+resultTab+ ' AS SELECT * FROM  (SELECT '+','.join(allVars1)+' FROM '+self.srcName+" UNION SELECT "+','.join(allVars1)+" FROM "+guy.srcName+" ) "
            #print query
            self.pConn.execute(query)
            self.pConn.commit()
            self.drop()
            self.srcName = resultTab
            self.srcType = resultType
        except Exception as err:
            print_error(err, 'pydset.union')   
            print (query)
            print (self.colNames)
            print (guy.colNames)
            print self.head(1)
            print guy.head(2)
            exit(0)



    def update(self, colName, newVal, conditions=['1=1'],returnQueryOnly=False):
        '''
          Assume Table : FirstName, LastName, Description
          Example: Set 'Description' to 'Idiot' where 'LastName' is 'Gaandhi' and 'FirstName' is 'Raahul'.
        '''
        try:
            if 'TABLE' not in self.srcType.upper():
                raise ValueError('pydset.update_column: operation permitted for tables only!')
            else:
                query =  'UPDATE '+self.srcName+' SET '+colName+' = '+str(newVal)+' WHERE '+' AND '.join(conditions)
                #print (query)
                if (returnQueryOnly == True):
                    return query
                self.pConn.cursor().execute(query)
                self.pConn.commit()
        except Exception as err:
            print_error(err, 'pydset.update')
            
    def apply(self, colName, funcName, func, argNames, conditions=['1=1'],
              returnQueryOnly=False):
        try:
            argName = ','.join(argNames)
            numparams = len(argNames)
            self.pConn.create_function(funcName,func=func,narg=numparams)
            self.update(colName,newVal=funcName+'('+argName+')',
                                conditions=conditions,
                                          returnQueryOnly=returnQueryOnly)
        except Exception as err:
            print_error(err,'pydset.apply')
            
            
            
    def addColumn(self, newColumn, newColType, funcName, func, argNames, conditions=["1=1"]):
        try:
            query = "ALTER TABLE "+self.srcName+" ADD COLUMN ?newColumn? ?newColumnType?"
            query = query.replace("?newColumn?",newColumn)
            query = query.replace("?newColumnType?",newColType)
            self.colNames.append(newColumn)
            self.colTypes[newColumn]=newColType
            self.pConn.cursor().execute(query)
            self.apply(newColumn,funcName,func,argNames,conditions)
        except Exception as err:
            print_error(err, "pydset.addColumn")
    
    
    
    def subset(self,resultTab,resultType=' TABLE ',varNames=['*'],srcName='',
                    conditions = ['1=1'],groupBy = [],orderBy = [],limit=-1, 
                    offset=-1,returnQueryOnly=False):
        '''
         Writes a subset of data, as a TABLE or TABLE and returns a pydset object bound to new table.        
        '''
        try:
            myVarNames=[]  ##A temporary surrorgate
            for var in varNames:
                myVarNames.append(var.strip())
            #    if var.strip() == '*':
            #        for v in self.colNames: myVarNames.append(v)
            #    else : myVarNames.append(var)
            varNames = myVarNames    
            ##Drop if there already is a table with name 'resultTab'
            ##self.pConn.cursor().execute(drop_table_qry(resultTab,'TABLE'))
            print drop_table_qry(resultTab,resultType)
            self.pConn.cursor().execute(drop_table_qry(resultTab,resultType))
            
            self.pConn.commit()
            if srcName=='':
                srcName = self.srcName
            else:
                srcName = '('+srcName+')'
            query = select_data_qry(varNames=varNames,
                                    srcNames=[srcName],
                                    conditions=conditions,
                                    orderby=orderBy,
                                    groupby=groupBy,
                                    limit = limit,
                                    offset= offset)
            query = '\nCREATE ' + resultType + ' '+resultTab+" AS "+query
            if returnQueryOnly == True:
                return query 
            ##print (query)
            
            self.pConn.cursor().execute(query)
            self.pConn.commit()
            #self.pConn.execute('PRAGMA synchronous=NORMAL');
            if self.dbaseName.strip()==':memory:':
                conn = self.pConn
            else: conn = None
            pv = pydset(dbaseName=self.dbaseName,srcName=resultTab, 
                               srcType=resultType, conn = conn)
#            pv = pydset(self.dbaseName,resultTab)
#           pv.srcName = resultTab
#             pv.srcType = resultType             
            for varName in pv.colTypes:
                 if varName in self.colTypes:
                     pv.colTypes[varName] = self. colTypes[varName]
            #print (pv.srcName)
            return pv
        except Exception as err:
            print_error(err,'pydset.subset')

    def transform(self,resultTab,colTypes, resultType = " TABLE ",conditions=['1=1'],
                                             returnQueryOnly=False):
        '''
         colTypes={varName:varType}
         Return table of varNames in colTypes, with proper type information.
        '''
        try:
            varNames =[]
            for varName in colTypes:
                varType = colTypes[varName]
                if varType.lower().strip() == 'NUMBER'.lower():
                    varNames.append('1*'+varName+'  '+varName)
                elif varType.lower().strip()=='DATE'.lower():
                    varNames.append('DATE('+varName+')  '+varName)
                else: varNames.append(varName)
            pv = self.subset(resultTab=resultTab, resultType=resultType, 
                                        varNames=varNames,conditions=conditions,
                                        returnQueryOnly=returnQueryOnly)
            for var  in colTypes:
                pv.colTypes[var]=colTypes[var]
            return pv
        except Exception as err:
            print_error(err, 'pydset.transform')
            
    def get_chunk(self,resultTab, resultType=' TABLE ',fLine=1, lLine=2, 
                       varNames=[], conditions = ['1=1'],orderBy=[],
                                    returnQueryOnly=False):
        '''
          Set a subset of data between fLine, lLine subject to certain conditions. 
          Example: Order people by number of years in jail, return the guys 
          between rank-1 to rank-17
        '''
        try:        
            if fLine < 1:
                raise ValueError("fLine has to be greater than 0")
            firstLine =int(fLine)
            numLines = int(lLine)-firstLine+1
            ofset = firstLine-1
            limit = numLines 
            return  self.subset(varNames=varNames, 
                         conditions=conditions, 
                         limit=limit,
                         offset=ofset,
                         resultTab=resultTab,
                         resultType=resultType,
                         orderBy = orderBy,
                         returnQueryOnly=returnQueryOnly
                         )
        except Exception as err:
            print_error(err, 'pydset.chunk')
            
    @staticmethod
    def join_qry(rsltTab, rsltType, d1,d2,colNames1,colNames2,on=['1=1'],conditions=['1=1']):
        try:
            t1 = d1.srcName
            t2 = d2.srcName
            varNames = [t1+'.'+x for x in colNames1]+[t2+'.'+x for x in colNames2]
            var = ',\n '.join(varNames)
            query = 'CREATE  '+rsltType+ '  '+rsltTab+' AS \n'
            query += 'SELECT\n'
            query += var
            query += '\n FROM \n'+t1+' INNER JOIN '+t2
            query += '\n ON \n'
            query += '\n AND '.join(on)
            query += '\n WHERE\n '+'\n,'.join(conditions)
            return query
        except Exception as err:
            print_error(err, 'join_qry') 

    def join(self, resultTab,d2,varNames1,varNames2,on=['1=1'],joinType=' JOIN ',resultType=" TABLE ",returnQueryOnly=False):
            try:
               varNames11 = ['d1.'+var for var in varNames1]
               varNames12 = ['d2.'+var for var in varNames2]
               
               for j in on:
                 vs = j.strip().split('=')
                 v1  = vs[0].strip()
                 v2  = vs[1].strip()
                 q1 = 'CREATE INDEX IF NOT EXISTS index_'+v1+' ON '+self.srcName+' ('+v1+')'
                 q2 = 'CREATE INDEX IF NOT EXISTS index_'+v2+' ON '+d2.srcName+' ('+v2+')'
                 #print q1
                 #print q2
                 self.pConn.execute(q1)
                 self.pConn.execute(q2)   
                
               
               
               onn = []
               for condition in on:
                   if condition.strip()=='1=1': pass
                   else:
                       cond = condition.split('=')
                       if len(cond)!=2: pass
                       else: onn.append(
                             '='.join(['d1.'+cond[0].strip(),'d2.'+cond[1].strip()]))
               varNames = varNames11+varNames12
               on = onn
               query = 'SELECT \n'
               query = query + ',\n'.join(varNames)
               query = query+'\nFROM \n'
               query = query+joinType.join((self.srcName+' as d1',d2.srcName+' as d2'))
               query = query+'\nON\n'
               query = query+' AND '.join(on)      
               #print query 
               if returnQueryOnly==True:
                   return query
               else:
                   return self.subset(resultTab,resultType,varNames=['*'],srcName=query,
                        conditions = ['1=1'],returnQueryOnly=False)
            except Exception as err:
                print_error(err,'pydata.join') 
    
    


                  
    def unpivot(self, resultTab, rsltCol1Name, rsltCol2Name,rsltCol1Type, dynamicCols,fixedCols=[],returnQueryOnly=False):
        '''
         Increase number of rows by converting multiple columns of related data into row form.
        '''
        try:
            if (len(dynamicCols)==0):
                raise ValueError('pydset.column_to_rows: dynamicCols can not be empty')
            ##Check if all the columns under union have same type.
            allSameType= True
            dataType = self.colTypes[dynamicCols[0]]            
            for col in dynamicCols:
                allSameType = allSameType and (self.colTypes[col] == dataType)
            if allSameType == False:
                raise ValueError('pydset.column_to_rows: dynamicCols should be of same type') 
            ##Let us determine which column names are not going to change.
            fixedColTypes = []    
            if len(fixedCols)==0:  
                for col in self.dynamicCols:
                    if col in dynamicCols:
                        pass
                    else:
                        fixedCols.append(col)
                        fixedColTypes.append(self.colTypes[col])
            else:
                fixedColTypes = [self.colTypes[col] for col in fixedCols]
            ##Check of the fixed columns lead to unique tuples.
            #print self.srcName
            uniqueCount = self.pConn.execute(
                    'SELECT COUNT(*) FROM (SELECT DISTINCT '
                  +','.join(fixedCols)+' FROM '+self.srcName+')').fetchone()[0]
            #print 'KI'

            #print 'SELECT COUNT(*) FROM (SELECT'+','.join(fixedCols)+' FROM '+self.srcName+')'
            
            totalCount  = self.pConn.execute(
                            'SELECT COUNT(*) FROM (SELECT '+','.join(fixedCols)
                            +' FROM '+self.srcName+')').fetchone()[0]
            #print 'HERE'               
            if uniqueCount != totalCount:
                raise ValueError('pydset.column_to_rows: fixedCols must yield a stable set of rows')       
            ##Get on with the actual transposing.
            queryBox= []    
            for i in range(0,len(dynamicCols)):
                q = select_data_qry(
                                    varNames=fixedCols+["'"+dynamicCols[i]+"' "+rsltCol1Name, 
                                                          dynamicCols[i]+ '  '+rsltCol2Name],
                                    srcNames = [self.srcName]
                                    )
                queryBox.append(q)
            query = '\nUNION ALL\n'.join(queryBox)
            query = 'CREATE TABLE '+resultTab+' AS \n'+query
            if returnQueryOnly == True:
                return query
            #print query
            self.pConn.execute('DROP TABLE IF EXISTS '+resultTab)
            self.pConn.execute(query)   
            pv = pydset(self.dbaseName,resultTab,' TABLE ')
            return pv
 
        except Exception as err:
            print_error(err, 'pydset.columns_to_rows')
    
            
    def aggregate(self,resultTab,resultType=' TABLE ',aggOp=' SUM ', varNames=['*'],conditions = ['1=1'],groupBy=[],orderBy = [],returnQueryOnly=False):
        '''
         Collapse multiple rows using aggOp operation, write to sqlite and return pydset object.
         Demonstration: 
            Given a dataset like:
                store1, item1, sale11
                store1, item2, sale12
                store1, item3, sale13
                store2, item1, sale21
                store2, item2, sale22 
            Use this function to obtain:
                store1, total_sale_store1
                store2, total_sale_store2
            or to obtain:
                item1, total_sale_item1
                item2, total_sale_item2
                item3, total_sale_item3
            or to obtain:
                total_sales_allItems_allStores             
        '''
        try:
            sumVars = []       
            aggOp = aggOp+'( '
            if ((len(varNames)==1 and varNames[0]=='*') or len(varNames)==0):
                sumVars = [(aggOp+var+') '+var) for var in self.colNames]
                groupBy = []
            else:
                sumVars = [(aggOp+var+') '+var) for var in varNames]
          
            myvars = [var for var in groupBy]
            ##if its ugly so be it... i dont give a shit.
            for sv in sumVars:
                myvars.append(sv)
            #print myvars
            return self.subset(resultTab=resultTab,
                          resultType=resultType,
                          varNames=myvars,
                          conditions=conditions,
                          groupBy = groupBy,
                          orderBy = orderBy,
                          returnQueryOnly=returnQueryOnly
                          )
        except Exception as err:
            print_error(err,'pydset.aggregate')
            
    def get_sum(self,resultTab,resultType=' TABLE ', varNames=['*'],conditions = ['1=1'],
                               groupBy=[],orderBy = [],limit=-1, offset=-1,returnQueryOnly=False):
        '''
         Calculate SUM of each variable in varNames, as a function of variables in groupBy. 
          Write summary to sqlite and return pydset object.
        '''
        
        try:
            return self.aggregate(
                                  resultTab = resultTab,
                                  resultType = resultType,
                                  aggOp = ' SUM ',
                                  varNames=varNames,
                                  conditions=conditions,
                                  groupBy=groupBy,
                                  orderBy=orderBy,
                                  limit=limit,
                                  offset=offset,
                                  returnQueryOnly=returnQueryOnly
                                  )
        except Exception as err:
            print_error(err, 'pydset.sum')
                           
    def get_count(self,resultTab,resultType= ' TABLE ',groupBy=[],orderBy = []):
        '''
        Writes frequencies correspoding to each unique combination of variables in 'groupBy' to sqlite and return pydset object. 
        '''  
        try:             
            cVars = [var for var in groupBy]
            cVars.append(' COUNT('+cVars[0]+') as freq')                
            vw = self.subset(
                          resultTab = resultTab,
                          resultType = resultType,
                          varNames=cVars,groupBy=groupBy
                          )
            
            vw.colTypes['freq']='NUMBER'
            return vw
        
        except Exception as err:
            print_error(err, 'pydset.count_by')
    ################################################################################
    #Below are the functions that are from scalar family. All of them are actually
    #wrappers about the pydata.scalar function and always return a single number.
    #
    #
    #
    ################################################################################
    def __scalar(self,varString,conditions = ['1=1']):
        try:
            query = select_data_qry(varNames=[varString],srcNames=[self.srcName],conditions = conditions)                      
            result = self.pConn.cursor().execute(query)
            return result.fetchone()[0]            
        except Exception as err:
            print_error(err, 'pydset.scalar')
    
    def get_value(self, varString, primKeyConditions=['1=1'] ):
        try:
            return self.__scalar(varString,primKeyConditions)
        except Exception as err:
            print_error(err, 'pydset.get_value')
    
    def total(self,varName,conditions=['1=1']):
        try:
            return self.__scalar(varString='SUM('+varName+')',
                                 conditions=conditions)
        except Exception as err:
            print_error(err,'pydset.total')
            
    def count(self,varName='*', conditions = ['1=1']): 
        try:
            return self.__scalar('COUNT('+varName+')', conditions)            
        except Exception as err:
            print_error(err,'pydset.count')
                   
    def mean(self, varName, cnt=-1, conditions=['1=1']):
        try:
            s = self.total(varName,conditions)
            if cnt < -1:
                cnt = self.count(varName,conditions)
            return s/cnt
        
        except Exception as err:
            print_error(err,'pydset.mean')
                
    def std(self,varName,mean=-1.0,count=-1,conditions=['1=1']):
        try:
            ##If you don't give me a mean, i used my own.
            if count == -1:
                count = self.count(varName,conditions)
            if mean == -1:
                mean = self.mean(varName,conditions,count=count)
            varString = ' ('+str(varName)+' - '+str(mean)+')*('+str(varName)+' - '+str(mean)+') '
            sumSq = self.total(varString, conditions)
            ##stdDev = math.sqrt(sumSq/count)
            return sumSq
        except Exception as err:
            print_error(err, 'pydset.std')
                 
    def min(self,varName, conditions=['1=1']):
        try:
            return self.__scalar('MIN('+varName+')', conditions)
        except Exception as err:
            print_error(err,'pydset.min')    
    
    def max(self,varName, conditions=['1=1']):
        try:
            return self.__scalar('MAX('+varName+')', conditions)
        except Exception as err:
            print_error(err, 'pydset.max')
    
    def quantile(self,varName,probability, conditions=['1=1']):
        '''
        return sample quantile for probability in dataset subject to
        conditions. 
        '''
        try:
            nRows = self.count(conditions=conditions)
            if (nRows==0):return 0
            index = 1+(probability*(nRows-1))
            floor = int(math.floor(index))
            ceill  = int(math.ceil(index))
            ####print 'C: ',index,',',floor,',',ceill
            limit =1
            offset = 1
            if index==floor==ceill:
                limit =1
                offset = index-1
            else:
                limit = 2
                offset = floor-1
            
            query = select_data_qry(
                        varNames=[varName],
                        srcNames=[self.srcName],
                        conditions=conditions,
                        orderby=[varName],
                        limit = limit,
                        offset = offset)
            ##print query
            result = self.pConn.cursor().execute(query)
            if index==floor==ceill:
                return result.fetchone()[0]
            else :
                fVal = result.fetchone()[0]
                cVal = result.fetchone()[0]
                return (index-floor)*cVal + (ceill-index)*fVal
        except Exception as err:
            print_error(err,'pydset.percentile')
        
    def median(self, varName,conditions=['1=1']):
        try:
            return self.quantile(varName, probability=0.5,
                                           conditions=conditions)
        except Exception as err:
            print_error(err, 'pydset.median') 
            
            
    ###Anil : Below are some fun functions who are provided to be able to explore the 
    ### data. They might not be very useful for production level analytic systems but
    ### are of great values while one tries to sniff data and design an analysis.
    #Below are the functions that simply print things on screen. They are
    #really handy for the exploratory analysis but should not form a part of
    #regular automated work chain.
    #
    ############################################################################    
    def show (self,fLine=1, lLine=-1, varNames=[],conditions = ['1=1'],dsetName = ''):      
        allVars = ''
        if len(varNames)==0 or ('*' in varNames):
            allVars = ','.join(self.colNames)
        else :
            allVars = ','.join(varNames)                    
        query = 'SELECT * FROM (SELECT '+allVars+' FROM '+self.srcName +')'       
        if lLine > 0:                       
            firstLine = fLine
            numLines = lLine-fLine+1
            ofset = firstLine-1
            limit = numLines           
            query = 'SELECT * FROM (SELECT '+allVars+' FROM '+self.srcName+") LIMIT "+str(limit)+" OFFSET "+str(ofset)
            
        try:
            #print query
            results = self.pConn.cursor().execute(query)
            print (cosmetic_line(len(self.colNames), 19))
            
            print_tuple(tuple(allVars.split(',')))
            print (cosmetic_line(len(self.colNames), 19))
            for row in results:
               
                print_tuple(row)
            print (cosmetic_line(len(self.colNames), 19))
        except Exception as err:
            print_error(err,'pydset.show')      
    
    def head(self,num=-1):
        print ("\nWARNING (pydset.head): Tableing-only tool invoked!")
        if num==-1:
            num = self.count()
        fLine = 1
        lLine = fLine+num-1
        self.show(fLine,lLine)
    
    def tail(self,num):
        print ("\nWARNING (pydset.tail): Viewing-only tool invoked!")
        count = self.count()        
        fLine = count-num+1
        lLine = count
        self.show(fLine, lLine)

    def columns(self, printIt=True):
        '''
          Arguments: None
          Description: Prints the name of columns and the corresponding
                datatypes as per current understanding.
        '''
        ret_dict = self.colTypes.copy()
        for key in ret_dict:
            if ret_dict[key]=='':
                ret_dict[key]='NUMBER'
        if printIt == True:
            print (cosmetic_line(2,19))
            print_tuple(("Variable","Type"),20)
            print (cosmetic_line(2,19))        
            for key in self.colTypes:
                print_tuple((key,ret_dict[key]),20)
            print (cosmetic_line(2,19))        
        return ret_dict
        
    def describe(self,varNames=[]):
        def describe_column(varName):       
            try:            
                metrics = {}
                metrics['varName']=varName
                cnt = self.count(varName)
                metrics['count']  = cnt
                metrics['sum']=self.total(varName)
                metrics['min']=self.min(varName)
                metrics['max']=self.max(varName)
                m= self.mean(varName, cnt)
                metrics['mean']=m
                metrics['stddev']=self.std(varName,m,cnt)
                metrics['median']=self.median(varName)
                return metrics
            except Exception as err:
                print_error(err,'pydset.describe.describe_column' )

        try:    
            cosmetic = cosmetic_line(7,19)
            print (cosmetic)
            print_tuple(('Variable','Mean','Min','Max','Median', 'std','count'),20)
            print (cosmetic)
            if len(varNames)==0 or ('*' in varNames):
                varNames = []
            for x in self.colNames:
                if 'PYDAN_ROW_NUM' in x or (self.colTypes[x] in ('VARCHAR','DATE','TEXT')):
                    pass
                else:                
                    varNames.append(x)
            for varName in varNames:
                if self.colTypes[varName].lower().strip() == 'NUMBER'.lower():
                    metrics = describe_column(varName)
                    print_tuple((str(metrics['varName']),
                               str(metrics['mean']),
                               str(metrics['min']), 
                               str(metrics['max']),
                               str(metrics['median']), 
                               str(metrics['stddev']), 
                               str(metrics['count'])),20)  
            print (cosmetic)
        except Exception as err:
            print_error(err, 'pydset.desribe')         
            
            
            
    def get(self,colName, distinct=False):   
        unique = ''
        if distinct==True: unique = ' DISTINCT '
        query = 'SELECT '+' '+colName+' FROM '+self.srcName+' WHERE '+colName+" != ''"
        ####print query
        results = self.pConn.cursor().execute(query)
        colVals = []
        for value in results:
            ##Either is a integer type expression or a float type expression
            ##val = clean_string(value[0],replaceHyphen= False)
            ##Why were you changing the values in above line... before returning?
            val = value[0]
            colVals.append(val)
            
        return colVals 

    def get2(self,colName1,colName2,distinct = False):
        unique = ''
        if distinct == True: unique = ' DISTINCT '
        query = 'SELECT '+' '+colName1+', '+colName2+' FROM '+self.srcName+' WHERE '+colName1+" != ''"
        results = self.pConn.cursor().execute(query)
        colVals1 = []
        colVals2 = []
        for row in results:
            v1 = row[0];
            v2 = row[1];
            colVals1.append(v1)
            colVals2.append(v2)
        return colVals1,colVals2

    
    def check_numericity(self, colName):
        def make_unicode(input):
            if type(input) != unicode:
                input =  input.decode('utf-8')
                return input
            else:
                return input
        try:
#            if ('VARCHAR'.lower() != self.colTypes[colName].lower().strip()):
#                 raise ValueError("pydset.check_numericity: Operation permitted for text columns only.")
            results = self.get(colName)
            isNumber = True
            colVals  = []
            for value in results:
                val = clean_string(str(value),replaceHyphen= False)
                uv = make_unicode(val)
                isNumber = isNumber and (uv.isdecimal() or uv.isnumeric())
            return isNumber 
        except Exception as err:
            print_error(err, 'pydset.check_numericity')
    
    def check_temporicity(self, colName, dateFormat='yyyy-mm-dd'):        
        try:
#             if self.colTypes[colName]!='VARCHAR':
#                 raise ValueError("pydset.check_numericity: Operation permitted for text columns only.")
            matcher = {'dd-mm-yyyy':'\d{1,2}-\d{1,2}-\d{4}','yyyy-mm-dd':'\d{4}-\d{1,2}-\d{1,2}'}
            isDate = True           
            results = self.get(colName)               
            for value in results:
                val = clean_string(str(value),replaceHyphen=False)
                isDate = isDate and (re.match(matcher[dateFormat],val)!=None)
            return isDate        
        except Exception as err:
            print (err)
            print_error(err,'dataset.check_temporicity')  
      
    def analyze_columns(self, dateFormat='yyyy-mm-dd'):        
   
        try:
            ##First identify numeric columns.
            colTypes={}
            for colName in self.colNames:
                isNum = self.check_numericity(colName)
                if (isNum):
                    colTypes[colName] = 'NUMBER'
                elif (self.check_temporicity(colName,dateFormat) == True):
                    colTypes[colName] = 'DATE'
                else :
                    colTypes[colName]='VARCHAR'
            return colTypes
        except Exception as err:
            print_error(err, 'pydset.analyze_col_types')

            
    def __discrete_count(self, resultTab,resultType = ' TABLE ',lowBinDict={}, 
                                  highBinDict={},conditions=['1=1'], 
                                               returnQueryOnly=False):
        '''
        lBinDict: {var1:[1,2,3,4], var2:[1,2,3,4,5]... so on}
        hBinDict: {var1:[2,3,4,5],var2:[2,3,4,5]... so on}
        '''
        try:
            query = get_binning_query(self.srcName,lowBinDict=lowBinDict,
                                      hiBinDict=highBinDict)
            
            
            varNames = [v for v in lowBinDict.keys()]
            
            varNames.append('COUNT(*) as FREQ')
            groupby = [ v for v in lowBinDict.keys()]
            hist = self.subset(
                            resultTab = resultTab,
                            resultType= resultType,
                            varNames  = varNames,                  
                            conditions = ['1=1'],
                            returnQueryOnly=returnQueryOnly
                            )
            return hist
        except Exception as err:
            print_error(err, 'pydset.__discrete_count')

    def skeleton(self, tabName, varNames=[]):       
        if self.pConn is None:
            raise RuntimeError("pycvs is not connected to a database yet")
        else:
            try:
                self.pConn.cursor().execute('drop table if exists '+tabName)
                self.pConn.commit()
                if len(varNames)==0:
                    varNames = self.colNames
                
                query = 'CREATE TABLE '+tabName+' (\n'
                variables = []
                for colName in varNames:
                    variables.append(colName + '  ' + self.colTypes[colName] )
                query = query+',\n'.join(variables)+')'
                ##print query
                self.pConn.cursor().execute(query)
                self.pConn.commit()
                pv = pydset(self.dbaseName,tabName)
                pv.srcType = ' TABLE '
            #print 'test2'
                for varName in pv.colTypes:
                    if varName in self.colTypes:
                        pv.colTypes[varName] = self. colTypes[varName]
                return pv
            except Exception as err:
                print_error(err, 'pydset.skeleton')


    def to_numpy(self,varNames, conditions=['1=1'],groupBy=[],orderBy=[],limit=-1,offset=-1):
        '''
          Returns a subset of data, as a numpy table
        '''
        try:
            query = select_data_qry(varNames,[self.srcName],conditions,groupBy,orderBy,limit,offset)
            retAr = []
            results = self.pConn.cursor().execute(query)
            for row in results:
                retAr.append(list(row))
            return np.array(retAr)
        
        except Exception as err:
            print_error(err,'pydset.to_numpy')

                
    def to_csv(self,fileName, varNames=['*'], conditions=['1=1'],groupBy=[],orderby=[],limit=-1,offset=-1):
        '''
         Writes a subset of data, as a csv file on disk.
        '''
        try:
            ofile = open(fileName,'w')  
            query = select_data_qry(varNames,[self.srcName],conditions,groupBy,orderby,limit,offset)
            #print query
            results = self.pConn.cursor().execute(query)
            varNames =  [ i[0] for i in results.description]  
            ofile.write(','.join(varNames)+'\n')
            for record in results:
                ofile.write(','.join(map(str,record))+'\n')
            ofile.close()
        except Exception as err:
            print_error(err,'pydset.to_csv')

    def to_database(self,tabName,varNames=['*'], conditions=['1=1'],groupBy=[],orderBy=[],limit=-1,offset=-1):
        '''
         Writes a subset of data, as a table in SQLITE file.
         No pydset object is returned.
         
         Note: Why do we even need this when we have self.subset? 
          consider deprecating.
        '''
        try:
            #print ("Going to create: "+tabName)
            query = ' CREATE TABLE '+tabName+' AS SELECT * FROM '+self.srcName
            self.pConn.execute('DROP TABLE IF EXISTS '+tabName)
            self.pConn.execute(query)
            self.pConn.commit()
            ##pd = pydset(self.dbaseName, tabName)
            ##pd.force_col_types(self.colTypes)
            ##return pd
        except Exception as err:
            print_error(err,'pydset.to_database')

    



    def hist1D(self,outfile,varName,histName,title, nBins=None,minRange=None,maxRange =None,lBinEdges=[],hBinEdges=[]):
        try:
            if (nBins != None and minRange != None and maxRange != None):
                binWidth = float(maxRange-minRange)/nBins
                lBinEdges =[]
                hBinEdges = []
                for n in range(0,nBins):
                    lBinEdges.append(n*binWidth+minRange)
                    hBinEdges.append(minRange+(n+1)*binWidth)
                #print lBinEdges
                ##lBinDict: {var1:[1,2,3,4], var2:[1,2,3,4,5]... so on}
   
                hDisc = self.__discrete_count(resultTab='hist'+varName, lowBinDict={varName:lBinEdges},highBinDict={varName:hBinEdges})
                rHist = TH1D(outfile,name=histName,title=title,lbins = lBinEdges, hbins=hBinEdges)
                query = 'select '+varName+', FREQ FROM '+ hDisc.srcName
                results = self.pConn.cursor().execute(query)
                for result in results:
                    lbin = result[0]
                    freq = result[1]
                    rHist.set_bin_content(lbin,freq)
                return rHist
        except Exception as err:
            print_error(err, 'pydset.hist1D')




    def graph1D(self,outfile,varNameX,varNameY, formatX='',formatY='',plotName='test',plotTitle='test',xType='NUMBER',yType='NUMBER'):
        try:
            query = 'SELECT '+varNameX+', '+varNameY+' FROM '+self.srcName+' ORDER BY '+varNameX
            results = self.pConn.cursor().execute(query)
            x = []
            fx = []
            for result in results:
                xVar = result[0]
                yVar = result[1]
                if self.colTypes[varNameX]=='DATE':
                    t = dt.datetime.strptime(xVar,formatX)
                    xVar = mdate.date2num(t)
                if self.colTypes[varNameY]=='DATE':
                    t = dt.datetime.strptime(yVar,formatY)
                    yVar = mdate.date2num(t)
                x.append(xVar)              
                fx.append(yVar)
            ###print x          
            ###print fx
            gGraph = TGraph(dbaseName=outfile,name=plotName,status='new',title=plotTitle,x=x,y=fx,xType=xType,yType=yType)
            return gGraph
        except Exception as err:
            print_error(err, 'pydset.graph1D')


def Main():
    
    ##sqlite3.connect("C:\\Users\\SingAn22\Desktop\Temp\test.db")
    d = pydset(dbaseName="C:\\Users\\SingAn22\Desktop\\Temp\\test.db",srcName='t1', srcType=' TABLE ')
    ##d2 = pydset(dbaseName="C:\\Users\\SingAn22\Desktop\\Temp\\test.db",srcName='tt1', srcType=' TABLE ')
    ##pydset.join(rsltTab="test", rsltType="table", d1=d, d2=d2, colNames1=['a','b','c'], colNames2=['n','nval'])
    
    dd = d.columns_to_rows(resultTab="tet2", rsltCol1Name="n",rsltCol2Name="val",rsltCol1Type=" VARCHAR ",
                      colNames=['n1','n2','n3'])
    
    d.head()
    dd.head()


    
    
    
    
    
    
    
