'''
Created on Oct 22, 2015

@author: Anil Singh
'''
import sqlite3
import matplotlib.pyplot as plt
##from pydan.pydata import pydataview
from pypaint.pyaxis import TCosmetic


        
class TH1D(object):
    '''
    ONE DECISION THAT NEED TO MAKE IS THAT WETHER WE
    WANT TO MAKE HISTORGRAM A TABLE BASED DATA STRUCTURE 
    OR JUST A CONVINIENT WRAPPER ABOUT THE NUMPY HISTOGRAM
    OBJECT.
    
    AT THE MOMENT I HAVE CHOSEN TO GO BY WAY OF USING
    TABLES BASED DESIGN.. TIME WILL TELL IF THIS ONE 
    WILL EVER WORK.
    '''
    
    def __init__(self,dbaseName,name,status='new',**kwargs):
       
        self.dbaseName = dbaseName;
        self.pConn = sqlite3.connect(dbaseName)
        self.pCosmetics = TCosmetic()
        self.keysTab = name+'keys'
        self.dataTab = name+'data'
        
        if status == 'new':
            title = kwargs['title']
            lbins = kwargs['lbins']
            hbins = kwargs['hbins']
            self.pConn.execute('DROP TABLE IF EXISTS '+self.keysTab)
            query = 'CREATE TABLE '+self.keysTab+'( NAME VARCHAR, TITLE VARCHAR, DATATAB VARCHAR)'
            ##print query
            self.pConn.execute(query)
            self.pConn.commit()
            query = 'INSERT INTO '+self.keysTab+" VALUES ('"+name+"', '"+title+"', '"+name+"data')"
            ##print query
            self.pConn.execute(query)
            query = 'CREATE TABLE '+name+'data'+' (binLowEdge NUMBER, binHighEdge NUMBER, CONTENT NUMBER, ERROR NUMBER)'
            self.pConn.execute('DROP TABLE IF EXISTS '+name+'data')
            self.pConn.execute(query)
            for l,h in zip(lbins,hbins):
                query = 'INSERT INTO '+name+'data'+' VALUES('+str(l)+', '+str(h)+', 0, 0)'
                self.pConn.execute(query)
            self.pConn.commit()            
            
        
    def fill(self,value=None,**kwargs):
        if value != None:
            query = 'UPDATE '+self.dataTab+' SET CONTENT =( CONTENT+1) WHERE ( '+str(value)+'>= binLowEdge and '+str(value)+' < binHighEdge)'
            self.pConn.execute(query)
            self.pConn.commit()
            
    def get_cosmetics(self):
        return self.pCosmetics   
            
    def set_name(self, name):
        query ='UPDATE '+self.keysTab +' SET NAME = '+name
        self.pConn.execute(query)
        self.pConn.commit()
        
    def set_title(self, title):
        query ='UPDATE '+self.keysTab +' SET TITLE = '+title
        self.pConn.execute(query)
        self.pConn.commit()
    
    def set_bin_content(self,lbin,value):
        query = 'UPDATE '+self.dataTab+ ' SET CONTENT=('+str(value)+') WHERE ( '+str(lbin)+'= binLowEdge )'
        self.pConn.execute(query)
        self.pConn.commit()
    
    def set_bin_error(self,lbin,err):
        query = 'UPDATE '+self.dataTab+ ' SET ERROR=('+str(err)+') WHERE ( '+str(lbin)+'= binLowEdge )'
        self.pConn.execute(query)
        self.pConn.commit()
    
    
    def __scalar(self, token, conditions=['1=1']):
        query = 'SELECT '+token+' FROM '+self.keysTab+' WHERE '+' and '.join(conditions)
        results = self.pConn.cursor().execute(query)
        return results.fetchone()[0]
    
    def get_name(self):
        return self.__scalar('NAME')
    
    def get_title(self):
        return self.__scalar('TITLE')
    
    def get_bin_content(self,lbin):
        cond = [str(lbin)+' = binLowEdge']
        return self.__scalar('CONTENT',conditions=cond)    
    
    def get_bin_error(self,lbin):
        cond = [str(lbin)+' = binLowEdge']
        return self.__scalar('ERROR',conditions=cond)
    
    
    def __vector(self,token,conditions=['1=1'], orderby=''):
        query = 'SELECT '+token+ ' FROM '+self.dataTab
        if orderby != '':
            query = query+' ORDER BY '+orderby
        ##print query
        results = self.pConn.cursor().execute(query)
        vec =[]
        for row in results:
            vec.append(row[0])
        return vec
    
    def get_bin_lowedges(self):
        return self.__vector('binLowEdge',orderby='binLowEdge')    
    
    def get_bin_highedges(self):
        return self.__vector('binHighEdge',orderby='binLowEdge')
    
    def get_bin_list(self):
        bins = [self.get_bin_lowedges()[0]]
        for val in self.get_bin_highedges():
            bins.append(val)
        return bins
    
        

    def get_bin_centers(self):
        return self.__vector('binLowEdge+(binHighEdge-binLowEdge)*0.5 as binCenter', orderby='binLowEdge')
    
    def get_bin_contents(self):
        return self.__vector('content',orderby='binLowEdge')
    
    def draw(self,ax):
        binCenters = self.get_bin_centers()
        binContents = self.get_bin_contents()
        bins   = self.get_bin_list()
        ax.set_xlabel(self.pCosmetics.xlabel)
        ax.set_ylabel(self.pCosmetics.ylabel)
        ax.hist(
                binCenters,
                bins,
                weights=binContents,
                histtype = self.pCosmetics.style,
                color = self.pCosmetics.color,
                hatch = self.pCosmetics.hatch,
                alpha = self.pCosmetics.alpha,
                label = self.pCosmetics.label,
                edgecolor=self.pCosmetics.edgeColor,
                linewidth = self.pCosmetics.lineWidth               
                ) 
                
def Main():
    hist = TH1D("C:\\Users\\SingAn22\\Desktop\\outfile.db",name="hPt",title="pT of Electrons",lbins = [0,5,10,15], hbins=[5,10,15,20])
    hist.fill(2)
    hist.fill(11)
    hist.fill(12)
    hist.fill(13)
    hist.fill(6)
    hist.fill(7)
    hist.fill(18)
    hist.set_bin_content(10,15)
    print hist.get_name()
    print hist.get_title()
    print hist.get_bin_content(10)
