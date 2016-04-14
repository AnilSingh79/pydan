'''
Created on Oct 6, 2015

@author: Anil Singh
'''

import os
import sys
import sqlite3
from pydan.pydata import pydset
from pydan.pysql import create_table_qry, insert_qry
from pydan.pytools import clean_text, print_error, clean_string


class pycsv_reader:
    '''
     csv reader
    '''
   
    def __init__(self,fName,headers=[],separator = ',', firstLineIsHeader=True):
        self.pFirstLineIsHeader = firstLineIsHeader
        self.pSeparator = separator
        self.pFname = fName
        if len(headers) !=0 and firstLineIsHeader==True:
            raise RuntimeError('Ambiguous Header Specifications')
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
                #if len(firstLine) == len(headers):
                self.pHeader = headers
                #else:
                #    raise RuntimeError("Not implemented yet")
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
            print ("Unable to find file: "+str(fName))
    
    def __del__(self):
        '''
        Returning the resources.
        '''
        self.pFirstLineIsHeader = None
        self.pSeparator = None
        self.pFname = None
        self.pHeader = None
        self.close()
        self.pFile = None
        
    def close(self):
        try:
            self.pFile.close()            
        except Exception as err:
            sys.stderr.write('ERROR: %s\n' % str(err))


            
    def toStr(self):
        print (self.pFname)
        for col in self.pHeader:
            print (col)   
         
   
    
    def  to_database(self,tabName,database,varNames=[],varTypes=None,ur=False):
        '''writes the csv file to a table.
           ---returns a pydset object associated with table
        '''
          

        try:
            pConn = sqlite3.connect(database)
            pConn.execute('drop table if exists '+tabName)
            pConn.commit()
            if len(varNames)==0:
                varNames = self.pHeader
            if varTypes == None:
                varTypes = {}
                
            for var in varNames:
                #print var
                if var not in varTypes: varTypes[var]='VARCHAR'
                else: varTypes[var]=varTypes[var].strip()


            query = create_table_qry(tabName,varDict=varTypes
                                            ,uniqueIdFlag=False)
            manyFlagTemporary = True
            #print query
            pConn.execute(query)
            
            pConn.commit()
            line = self.pFile.readline().strip().split(self.pSeparator);
            uId = 1
            
            manyLines = []
            counter = 0;
            q = ['?']*len(varNames)
            qr = '('+','.join(q)+')'
            query = 'insert into '+tabName+'('+','.join(varNames)+') values '+qr
            ####I AM UGLY CLEAN ME UP... A LOT.
            ##print varNames
            pConn.execute('PRAGMA synchronous=OFF');
            while True:
                if not line:
                    break;
                
                if (len(line)==1 and line[0]==''):
                    line = self.pFile.readline()
                    continue
                else:
#                    select,line = self.
                    #print line
#                    if (select):
                    #print line

                    line = [l.replace("\xa0", " ") for l in line]
                    manyLines.append(tuple(line))
                    
                    if (counter%1)==0:
                        if counter == 0:
                            pass
                        else:
                            ##print ("Stored :"+str(counter))
                            pConn.cursor().executemany(query,manyLines)
                            manyLines =[]
                    #if(select):
                    counter = counter+1
                    uId = uId+1 #: This one srews up more than help
                    row = self.pFile.readline().strip()
                    #row = clean_string(row,replaceHyphen=False)
                    ##Put everything as ascii string.
                    #row=row.encode('utf-8').decode('utf-8','ignore').encode("utf-8")
                    ##row = row.encode('ascii','ignore')
                    #row = row.replace('"','')
                    line = row.split(self.pSeparator)
                    ##print line
                    ##line = self.pFile.readline().split(',')
            
            pConn.executemany(query,manyLines)
            ##Move the cursor back to top of the csv file.
            pConn.commit()
            pConn.commit()
            self.pFile.seek(0,0)
            if (self.pFirstLineIsHeader == True):
                ##Move the file cursor to second line.
                self.pFile.readline() ##just throw it away.
            pConn.execute('PRAGMA synchronous=NORMAL');
            pConn.commit()
            pConn.close()
            ####This one does not make sense.
            p =  pydset(database,tabName,srcType= ' TABLE ')
        
            return p
        
        except Exception as err:
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

