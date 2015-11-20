'''
Created on Nov 19, 2015

@author: Anil Singh
'''
import sqlite3
import matplotlib.pyplot as plt
from pypaint.pycosmetic import pycosmetics

class TGraph(object):
    def __init__(self,dbaseName,name,status='new',**kwargs):
        
        self.dbaseName = dbaseName;
        self.pConn = sqlite3.connect(dbaseName)
        self.pCosmetics = pycosmetics()
        self.keysTab = name+'keys'
        self.dataTab = name+'data'
        self.xType = None
        self.yType = None
        self.title = None 
        self.name = None
        def extract_arg(token):
            if token not in kwargs:
                raise ValueError(token+' missing\n')
            else: return kwargs[token]
        
        if status == 'new':
            self.name = name
            self.title = extract_arg('title')
            x = extract_arg('x')
            self.xType = extract_arg('xType')
            y = extract_arg('y')
            self.yType = extract_arg('yType')
            
            self.pConn.execute('DROP TABLE IF EXISTS '+self.keysTab)
            query = 'CREATE TABLE '+self.keysTab+'( NAME VARCHAR, TITLE VARCHAR, XTYPE VARCHAR, YTYPE VARCHAR, DATATAB VARCHAR)'
            ##print query
            self.pConn.execute(query)
            self.pConn.commit()

            query = 'INSERT INTO '+self.keysTab+" VALUES ("+"','".join(["'"+name,self.title,self.xType,self.yType,self.dataTab+"'"])+")"
            print query
            ##print query
            self.pConn.execute(query)
            query = 'CREATE TABLE '+self.dataTab+' (x '+self.xType+', y '+self.yType+ ', ERROR NUMBER)'
            self.pConn.execute('DROP TABLE IF EXISTS '+self.dataTab)
            print query
            self.pConn.execute(query)
            
            for v,val in zip(x,y):
                query = 'INSERT INTO '+self.dataTab+' VALUES('+str(v)+', '+str(val)+',0)'
                self.pConn.execute(query)
            self.pConn.commit()            
    
    def __vector(self,token,conditions=['1=1'], orderby=''):
        query = 'SELECT '+token+ ' FROM '+self.dataTab
        if orderby != '':
            query = query+' ORDER BY '+','.join(orderby)
        ##print query
        results = self.pConn.cursor().execute(query)
        vec =[]
        for row in results:
            vec.append(row[0])
        return vec
 
    def get_y(self):
        return self.__vector('y', conditions=['1=1'], orderby=['x'])
    
    def get_x(self):
        return self.__vector('x',conditions=['1=1'],orderby=['x'])
    
    def get_cosmetics(self):
        return self.pCosmetics
    
    def draw(self,ax):
        xPoints = self.get_x()
        yPoints = self.get_y()
        print xPoints;
        print yPoints;
        ax.set_xlabel(self.pCosmetics.xlabel)
        ax.set_ylabel(self.pCosmetics.ylabel)
        
        self.add(ax)    
        ax.set_xlim([self.pCosmetics.xMin, self.pCosmetics.xMax])
        ax.set_ylim([self.pCosmetics.yMin, self.pCosmetics.yMax])    
            #for l in ax.get_xticklabels():
            #    l.set_rotation(00)
            #ax.xaxis.set_major_locator(mdates.YearLocator())
            #ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
            
    def add(self,ax):
        xPoints = self.get_x()
        yPoints = self.get_y()
        if not (self.xType == 'DATE' or self.yType=='DATE'):
            ax.plot(
                    xPoints,
                    yPoints,
                    ###histtype = self.pCosmetics.style,
                    color = self.pCosmetics.color,
                    ##hatch = self.pCosmetics.hatch,
                    alpha = self.pCosmetics.alpha,
                    label = self.pCosmetics.label,
                    ###edgecolor=self.pCosmetics.edgeColor,
                    linewidth = self.pCosmetics.lineWidth                                   
                    ) 
        else:
            ax.plot_date(
                    xPoints,
                    yPoints,
                    #self.pCosmetics.style,                    
                    color = self.pCosmetics.color,
                    ##hatch = self.pCosmetics.hatch,
                    alpha = self.pCosmetics.alpha,
                    label = self.pCosmetics.label,
                    
                    linewidth = self.pCosmetics.lineWidth  ,   
                    linestyle =   '-',
                    ##label = self.title
                    )
            
            
            #for l in ax.get_xticklabels():
            #    l.set_rotation(00)
            #ax.xaxis.set_major_locator(mdates.YearLocator())
            #ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

def Main():
    g = TGraph(dbaseName='test.db',name='tGraph',status='new',title='x',x=[1,2,3,4],fx=[1,4,9,16])
    g2 = TGraph(dbaseName='test.db',name='tGraph2',status='new',title='x',x=[1,2,3,4],fx=[1,8,27,64])
    g2.get_cosmetics().set_color('red')
    g2.get_cosmetics().set_linewidth(4)
    g.get_cosmetics().set_linewidth(4)
    fig = plt.figure()
    ax  = fig.add_subplot(1,1,1)
    g.draw(ax)
    g2.draw(ax)
    
    plt.show()

