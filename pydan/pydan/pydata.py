'''
Created on Oct 7, 2015


@author: Anil Singh, for Daddy's Ersa :)
'''


import sqlite3
#import math
import re
import numpy as np
import datetime as dt
import matplotlib.dates as mdate

##PYDAN IMPORTS
from pydan.pysql     import  create_table_qry  
from pydan.pysql     import  drop_table_qry
from pydan.pysql     import  select_data_qry
#from pydan.pysql    import  update_column_qry
#from pydan.pysql    import  get_column_metrics_qry
from pydan.pysql     import  get_median_qry
#from pydan.pysql    import  get_column_err2_qry
from pydan.pysql     import  get_binning_query
from pydan.pytools   import  clean_text
from pydan.pytools   import  print_error
#from pydan.pytools  import  clean_split_string
from pydan.pytools   import  clean_string
#from pydan.pytools  import  straight_line
from pydan.pytools   import  cosmetic_line
from pydan.pytools   import  print_tuple
from pypaint.pyhist  import  TH1D
from pypaint.pygraph import  TGraph


class pydset(object):
    def __init__(self,dbaseName,srcName, srcType=' VIEW '):
        '''
        A pydset instance is initialized over an existing table in SQLITE file.
        '''
        try:
            self.dbaseName = dbaseName;
            self.pConn = sqlite3.connect(dbaseName)
            self.srcName = srcName
            self.srcType = srcType
            self.colNames = []
            self.colTypes = {}
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
       
    def drop(self):
        '''
        Drop the underlying table from database.
        '''
        try:
            query = drop_table_qry(tabName=self.srcName,tabType=self.srcType)
            self.pConn.cursor.execute(query)
            self.pConn.commit()
        except Exception as err:
            print_error(err,'drop')
                        
    def union(self, guy, resultTab, resultType=' VIEW ', on=[]):
        '''
        Append two pydset objects. The regular SQL UNION rules apply:
          1. Both objects have same number of columns.
          2. Corresponding columns must have same name.
          3. Corresponding columns must have same type.
        ''' 
        try:
            self.pConn.execute(drop_table_qry(resultTab,resultType))
            self.pConn.commit()
            query = 'CREATE ' + resultType +'  '+resultTab+ ' AS SELECT * FROM  (SELECT * FROM '+self.srcName+" UNION SELECT * FROM "+guy.srcName+" ) "
            temp= []
            for var in on:
                temp.append('T1.'+var+'= T2.'+var)
            ##print query
            self.pConn.execute(query)
            self.pConn.commit()
            self.srcName = resultTab
            self.srcType = resultType
            self.colNames = []
            d = {}
            for var in on:
                self.colNames.append(var)
                d[var] = self.colTypes[var]
            self.colTypes = d
        except Exception as err:
            print_error(err, 'pydset.union_to')   


    def update(self, colName, newVal, conditions=['1=1'],returnQueryOnly='FALSE'):
        '''
          Assume Table : FirstName, LastName, Description
          Example: Set 'Description' to 'Idiot' where 'LastName' is 'Gaandhi' and 'FirstName' is 'Raahul'.
        '''
        try:
            if 'TABLE' not in self.srcType.upper():
                raise ValueError('pydset.update_column: operation permitted for tables only!')
            else:
                query =  'UPDATE '+self.srcName+' SET '+colName+' = '+newVal+' WHERE '+' AND '.join(conditions)
                if (returnQueryOnly == True):
                    return query
                self.pConn.cursor().execute(query)
                self.pConn.commit()
        except Exception as err:
            print_error(err, 'pydset.update')
    
    def subset(self,resultTab,resultType=' VIEW ',varNames=['*'],srcName='',conditions = ['1=1'],groupBy = [],orderBy = [],limit=-1, offset=-1,returnQueryOnly=False):
        '''
         Writes a subset of data, as a VIEW or TABLE and returns a pydset object bound to new table.        
        '''
        try:
            if (len(varNames)==1 and varNames[0]=='*'):
                varNames = [var for var in self.colNames]
            ##Drop if there already is a table with name 'resultTab'
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
            self.pConn.cursor().execute(query)
            self.pConn.commit()
            pv = pydset(self.dbaseName,resultTab)
            pv.srcName = resultTab
            pv.srcType = resultType             
            for varName in pv.colTypes:
                if varName in self.colTypes:
                    pv.colTypes[varName] = self. colTypes[varName]
            return pv
        except Exception as err:
            print_error(err,'pydset.get')

    def get_chunk(self,resultTab, resultType=' VIEW ',fLine=1, lLine=2, varNames=[], conditions = ['1=1'],orderBy=[],returnQueryOnly=False):
        '''
          Set a subset of data between fLine, lLine subject to certain conditions. 
          Example: Order people by number of years in jail, return the guys between rank-1 to rank-17
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
                  
    def columns_to_rows(self, resultTab, rsltCol1Name, rsltCol2Name,rsltCol1Type, dynamicCols,fixedCols=[],returnQueryOnly=False):
        '''
         Increase number of rows by converting multiple columns of related data into row form.
        '''
        try:
            ##Raise value error if dynamicCols is empty
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
                fixedColTypes = [self.colType[col] for col in fixedCols]
            ##Check of the fixed columns lead to unique tuples.
            uniqueCount = self.pConn.execute(
                            'SELECT COUNT(*) FROM (SELECT DISTINCT '+','.join(fixedCols)+' FROM '+self.srcName+')').fetchone()[0]
            totalCount  = self.pConn.execute(
                            'SELECT COUNT(*) FROM (SELECT'+','.join(fixedCols)+' FROM '+self.srcName+')').fetchone()[0]
                            
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
            ##print query
            self.pConn.execute('DROP TABLE IF EXISTS '+resultTab)
            self.pConn.execute(query)   
            pv = pydset(self.dbaseName,resultTab,' TABLE ')
            return pv
 
        except Exception as err:
            print_error(err, 'pydset.columns_to_rows')
    
            
    def aggregate(self,resultTab,resultType=' VIEW ',aggOp=' SUM ', varNames=['*'],conditions = ['1=1'],
                                 groupBy=[],orderBy = [],returnQueryOnly=False):
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
                          varNames=varNames,
                          conditions=conditions,
                          groupBy = groupBy,
                          orderBy = orderBy,
                          returnQueryOnly=returnQueryOnly
                          )
        except Exception as err:
            print_error(err,'pydset.aggregate')
            
    def get_sum(self,resultTab,resultType=' VIEW ', varNames=['*'],conditions = ['1=1'],
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
                           
    def get_count(self,resultTab,resultType= ' VIEW ',groupBy=[],orderBy = []):
        '''
        Writes frequencies correspoding to each unique combination of variables in 'groupBy' to sqlite and return pydset object. 
        '''  
        try:             
            cVars = [var for var in groupBy]
            cVars.append(' COUNT('+cVars[0]+') as freq')                
            vw = self.subset(
                          resultTab = resultTab,
                          resultType = resultType,
                          varNames=cVars,groupby=groupBy
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
            return float(result.fetchone()[0])            
        except Exception as err:
            print_error(err, 'pydset.scalar')
    
    def get_value(self, varString, primKeyConditions=['1=1'] ):
        try:
            return self.__scalar(varString,primKeyConditions)
        except Exception as err:
            print_error(err, 'pydset.get_value')
    
    def count(self,varName='*', conditions = ['1=1']): 
        try:
            return self.__scalar('COUNT('+varName+')', conditions)            
        except Exception as err:
            print_error(err,'pydset.count')
                   
    def mean(self, varName, cnt=-1, conditions=['1=1']):
        try:
            s = self.sum(varName,conditions)
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
            sumSq = self.sum(varString, conditions)
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
    
    def median(self, varName,count=-1.0,conditions=['1=1']):
        try:
            if count<0:
                count = self.count(varName,conditions)
            query = get_median_qry(self.srcName,varName,count)
            results = self.pConn.cursor().execute(query)
            return results.fetchone()[0]
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
            print query
            results = self.pConn.cursor().execute(query)
            print cosmetic_line(len(self.colNames), 19)
            print_tuple(tuple(self.colNames))
            print cosmetic_line(len(self.colNames), 19)
            for row in results:                
                print_tuple(row)
            print cosmetic_line(len(self.colNames), 19)
        except Exception as err:
            print_error(err,'pydataview.show')      
    
    def head(self,num=-1):
        print "\nWARNING (pydataview.head): Viewing-only tool invoked!"
        if num==-1:
            num = self.count()
        fLine = 1
        lLine = fLine+num-1
        self.show(fLine,lLine)
    
    def tail(self,num):
        print "\nWARNING (pydataview.tail): Viewing-only tool invoked!"
        count = self.count()        
        fLine = count-num+1
        lLine = count
        self.show(fLine, lLine)

    def columns(self):
        '''
          Arguments: None
          Description: Prints the name of columns and the corresponding
                datatypes as per current understanding.
        '''
        ret_dict = self.colTypes.copy()
        for key in ret_dict:
            if ret_dict[key]=='':
                ret_dict[key]='NUMBER'
        print cosmetic_line(2,19)
        print_tuple(("Variable","Type"),20)
        print cosmetic_line(2,19)        
        for key in self.colTypes:
            print_tuple((key,ret_dict[key]),20)
        print cosmetic_line(2,19)        
        return ret_dict
        
    def describe(self,varNames=[]):
        def describe_column(varName):       
            try:            
                metrics = {}
                metrics['varName']=varName
                cnt = self.count(varName)
                metrics['count']  = cnt
                metrics['sum']=self.sum(varName)
                metrics['min']=self.min(varName)
                metrics['max']=self.max(varName)
                m= self.mean(varName, cnt)
                metrics['mean']=m
                metrics['stddev']=self.std(varName,m,cnt)
                metrics['median']=self.median(varName, count=cnt)
                return metrics
            except Exception as err:
                print_error(err,'pydataview.describe.describe_column' )

        try:    
            cosmetic = cosmetic_line(7,19)
            print cosmetic
            print_tuple(('Variable','Mean','Min','Max','Median', 'std','count'),20)
            print cosmetic
            if len(varNames)==0 or ('*' in varNames):
                varNames = []
            for x in self.colNames:
                if 'PYDAN_ROW_NUM' in x or (self.colTypes[x] in ('VARCHAR','DATE')):
                    pass
                else:
                    varNames.append(x)
            for varName in varNames:
                metrics = describe_column(varName)
                print_tuple((str(metrics['varName']),
                           str(metrics['mean']),
                           str(metrics['min']), 
                           str(metrics['max']),
                           str(metrics['median']), 
                           str(metrics['stddev']), 
                           str(metrics['count'])),20)  
            print cosmetic
        except Exception as err:
            print_error(err, 'pydataview.desribe')         
            
            
            
    def get(self,colName):
            query = 'SELECT '+colName+' FROM '+self.srcName+' WHERE '+colName+" != ''"
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
        
    def check_numericity(self, colName):
        def make_unicode(input):
            if type(input) != unicode:
                input =  input.decode('utf-8')
                return input
            else:
                return input
        try:
            if ('VARCHAR'.lower() != self.colTypes[colName].lower().strip()):
                raise ValueError("pydset.check_numericity: Operation permitted for text columns only.")
            results = self.get(colName)
            isNumber = True
            colVals  = []
            for value in results:
                val = clean_string(value,replaceHyphen= False)
                uv = make_unicode(val)
                isNumber = isNumber and (uv.isdecimal() or uv.isnumeric())
            return isNumber 
        except Exception as err:
            print_error(err, 'pydataset.check_numericity')
    
    def check_temporicity(self, colName, dateFormat='yyyy-mm-dd'):        
        try:
            if self.colTypes[colName]!='VARCHAR':
                raise ValueError("pydset.check_numericity: Operation permitted for text columns only.")
            matcher = {'dd-mm-yyyy':'\d{1,2}-\d{1,2}-\d{4}','yyyy-mm-dd':'\d{4}-\d{1,2}-\d{1,2}'}
            isDate = True           
            results = self.get(colName)               
            for value in results:
                val = clean_string(value,replaceHyphen=False)
                isDate = isDate and (re.match(matcher[dateFormat],val)!=None)
            return isDate        
        except Exception as err:
            print err
            print_error(err,'dataset.check_temporicity')  
      
    def analyze_columns(self, dateFormat='yyyy-mm-dd'):        
   
        try:
            ##First identify numeric columns.
            for colName in self.colNames:
                isNum = self.check_numericity(colName)
                
                if (isNum):
                    self.colTypes[colName] = 'NUMBER'
                elif (self.check_temporicity(colName,dateFormat) == True):
                    self.colTypes[colName] = 'DATE'
        except Exception as err:
            print_error(err, 'pydataset.analyze_col_types')

            
    def __discrete_count(self, resultTab,resultType = ' VIEW ',lowBinDict={}, highBinDict={},conditions=['1=1'], returnQueryOnly=False):
        '''
        lBinDict: {var1:[1,2,3,4], var2:[1,2,3,4,5]... so on}
        hBinDict: {var1:[2,3,4,5],var2:[2,3,4,5]... so on}
        '''
        try:
            query = get_binning_query(self.srcName,lowBinDict=lowBinDict,hiBinDict=highBinDict)
            
            
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
            print_error(err, 'pydataview.__discrete_count')
    
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
                print_error(err, 'pydataview.skeleton')

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
                #print hBinEdges
                hDisc = self.__discrete_count(srcName='hist'+varName, lowBinDict={varName:lBinEdges},highBinDict={varName:hBinEdges})
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

    def to_numpy(self,varNames=['*'], conditions=['1=1'],groupBy=[],orderBy=[],limit=-1,offset=-1):
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
            print query
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
         
         Note: Why do we even need this when we have self.subset? consider deprecating.
        '''
        try:
            query = ' CREATE TABLE '+tabName+' AS SELECT * FROM '+self.srcName
            self.pConn.cursor().execute(query)
            self.pConn.commit()
            pd = pydset(self.dbaseName, tabName)
            pd.force_col_types(self.colTypes)
            return pd
        except Exception as err:
            print_error(err,'pydset.to_database')
            

            


def Main():
    
    ##sqlite3.connect("C:\\Users\\SingAn22\Desktop\Temp\test.db")
    d = pydset(dbaseName="C:\\Users\\SingAn22\Desktop\\Temp\\test.db",srcName='t1', srcType=' TABLE ')
    ##d2 = pydset(dbaseName="C:\\Users\\SingAn22\Desktop\\Temp\\test.db",srcName='tt1', srcType=' TABLE ')
    ##pydset.join(rsltTab="test", rsltType="table", d1=d, d2=d2, colNames1=['a','b','c'], colNames2=['n','nval'])
    
    dd = d.columns_to_rows(resultTab="tet2", rsltCol1Name="n",rsltCol2Name="val",rsltCol1Type=" VARCHAR ",
                      colNames=['n1','n2','n3'])
    
    d.head()
    dd.head()
