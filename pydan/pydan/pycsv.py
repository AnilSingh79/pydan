'''
Created on Oct 6, 2015

@author: singan22
'''

import os
import sqlite3
import sys
from pydan.pysql import create_table_qry, insert_qry
from pydan.pydata import pydset
from pydan.pytools import clean_text, print_error, clean_string



class pycsv_reader(object):
    '''
     csv reader
    '''
    _THE_CRAZY_SENTINEL_ = object()
    def __init__(self,token=_THE_CRAZY_SENTINEL_):
        if token is self._THE_CRAZY_SENTINEL_:
            ##pass
            raise ValueError("Can not construct directly, use pycsvfactory instead")
        self.pSeparator = ','
        self.pFname = ''
        self.dBase = ''
        self.pHeader = []
        self.pConn = None
        self.pFile = None
        self.pFirstLineIsHeader = True
      
    def __del__(self):
        '''
        Returning the resources.
        '''
        self.close()
        
    def close(self):
        try:
            self.pConn.close()
            self.pFile.close()
            ##sys.stdout.write("Released Resources Succesfully\n")
        except Exception, err:
            sys.stderr.write('ERROR: %s\n' % str(err))

    def toStr(self):
        print self.pFname
        for col in self.pHeader:
            print col
        
    def connect(self, dbase_name, dialect='sqlite3'):
        self.dBase = dbase_name
        if self.pConn is not None:
            raise RuntimeError('Already connected to a database')
            return False
        try:
            self.pConn = sqlite3.connect(dbase_name)
            self.pConn.text_factory = str
            ##pCurs = self.pConn.cursor()
        except Exception, err:
            sys.stderr.write('ERROR: %s\n' % str(err))
        if self.pConn is None:
            raise RuntimeError('Failed to connect database '+dbase_name)
            return False
        else:
            ##raise RuntimeError('Connecting to database '+name)
            return True
    
    def  load(self,fName,headers=[],separator = ',', firstLineIsHeader=True):
        self.pFirstLineIsHeader = firstLineIsHeader
        self.pSeparator = separator
        if len(headers) !=0 and firstLineIsHeader==True:
            raise RuntimeError('Ambiguous Header Specifications')
        self.pFname = fName
        if os.path.exists(self.pFname):
            self.pFile = open(self.pFname,'r')
            firstLine   = self.pFile.readline().split(self.pSeparator)
            ##print firstLine
            if len(headers) == 0 and firstLineIsHeader==False:
                numCol = len(firstLine)
                for n in range (1,numCol):
                    self.pHeader.append(str(n))
            elif len(headers) == 0 and firstLineIsHeader==True:
                self.pHeader = firstLine
            elif len(headers) != 0 and firstLineIsHeader==False:
                if len(firstLine) == len(headers):
                    self.pHeader = headers
                else:
                    raise RuntimeError("Not implemented yet")
            self.pHeader = map (str.strip, self.pHeader)
            self.pHeader = map (clean_string,self.pHeader)
            ##The clean_text return two values: Check how it works here.
            ## Not easy to use map so let us replace things by list comprehension 
            ##self.pHeader = map (clean_text.,self.pHeader)
            ##@ANIL I don't want it this ugly
            tempHeader = []
            for cname in self.pHeader:
                a, h = clean_text(cname,self.pSeparator)
                a = a.replace('"','')
                tempHeader.append(a)
            self.pHeader = tempHeader
        elif IOError:
            print "Unable to find file: "+str(fName)
       
    def  to_database(self,tabName,varNames=[],varTypes=[]):
        '''writes the csv file to a table.
           ---returns a pydset object associated with table
        '''
        if self.pConn is None:
            raise RuntimeError("pycvs is not connected to a database yet")
        else:
            try:
                self.pConn.cursor().execute('drop table if exists '+tabName)
                self.pConn.commit()
                if len(varNames)==0:
                    varNames = self.pHeader
                query = create_table_qry(tabName,self.pHeader,varTypes,uniqueIdFlag=True)
                print query
                self.pConn.cursor().execute(query)
                self.pConn.commit()
                line = self.pFile.readline().split(self.pSeparator);
                uId = 1
                while True:
                    if not line:
                        break;
                    if (len(line)==1 and line[0]==''):
                        line = self.pFile.readline()
                        continue
                    else:
                        query = insert_qry(tabName,line,uniqueId=uId)
                        print query
                        self.pConn.cursor().execute(query)
                        uId = uId+1 #: This one srews up more than help
                        row = self.pFile.readline()
                        row = clean_string(row)
                        ##Put everything as ascii string.
                        row=row.encode('utf-8').decode('utf-8','ignore').encode("utf-8")
                        ##row = row.encode('ascii','ignore')
                        row = row.replace('"','')
                        line = row.split(self.pSeparator)
                        ##line = self.pFile.readline().split(',')
                ##Move the cursor back to top of the csv file.
                self.pConn.commit()
                self.pFile.seek(0,0)
                if (self.pFirstLineIsHeader == True):
                    ##Move the file cursor to second line.
                    self.pFile.readline() ##just throw it away.
                return pydset(self.dBase,tabName,srcType= ' TABLE ')    
            except Exception, err:
                print_error(err,"pycsv_reader.to_database")
            
    def reader(self, numLines=-9999):
        rows = []
        nLines = 0
        for row in self.pFile:
            r,rowList = clean_text(row, self.pSeparator)
            rows.append(rowList)
            nLines = nLines+1
            if(numLines != -9999 and nLines>=numLines):
                break
        self.pFile.seek(0,0)
        if (self.pFirstLineIsHeader == True):
            self.pFile.readline()
        return rows
    
    def dict_reader(self,numLines):
        data = self.reader(numLines)
        dataDict = []
        for row in data:
            ##Row is a list of variables.
            ##We zip it to headers to form a dictionary.
            dictRow = dict(zip(self.pHeader,row))
            dataDict.append(dictRow)
        return dataDict


    
        
class pycsvfactory(object):
    
    def __init__(self):
        pass
    
    def csv_reader(self,dbase_name,fName,headers=[],firstLineIsHeader=True,dbtype='sqlite3'):
        _the_crazy_sentinel_ = object()
        csv = pycsv_reader(_the_crazy_sentinel_)
        csv.connect(dbase_name, dbtype)
        csv.load(fName, headers, firstLineIsHeader)
        return csv
    
    def csv_writer(self,dbase_name,tabName,headers=[],firstLineIsHeader=True,dialect=''):
        '''
        Anil : Think if you wish to give this facility here or not... i am not sure
        '''
        pass