'''
Created on Nov 19, 2015

 Data pre-analysis: String level manipulation of data.
  1. Read CSV file into a SQLite database.
  2. Perform data cleaning and preparation.
  3. Deal with missing values
  2. Determine appropriate data-type for each column.

@author: Anil Singh
'''

from pydan.pycsv import pycsv_reader
from pydan.pytools import month_enum, print_error,clean_string
from pypaint.pygraph import TGraph
from matplotlib import pyplot as plt
from matplotlib import ticker
from datetime import datetime

def preAnalysis():
#   ##Initialize the csv reader
    csv = pycsv_reader("C:\\Users\\SingAn22\\Desktop\\hcsvreader\\533098.csv")
    ##Load the data into sqlite database, obtain pydset object.
    data = csv.to_database(tabName='pydanTable_prelim',
                           database ='C:\\Users\\SingAn22\\Desktop\\hcsvreader\\dset.db')
    ##Print the column names from data 
    data.columns()
    data.head(5)
    
    ##At the moment every element is a string, so analyze what datatypes can be associated with each string
    #Missing values are represented as empty strings, and they dont effect this operation.
                                 
    ##Let us print first few lines close to top... 
   
    ##We saw that 'Dat' column is not in a format recognised by sqlite. So we need to fix it.     
    #####1. Come up with a format fixing function.
    #####2. Update the Dat column by applying fixing function to each element. 
    '01-SEPTEMBER-2014'
    def refine_date(element):
        try:
            myDate = element.split('-')
            myMnth = '{:02d}'.format(int(month_enum(myDate[1])))
            myDay  = '{:02d}'.format(int(myDate[0]))
            myYear = myDate[2]
            newDate = '-'.join([myYear,myMnth,myDay])
            return newDate
        except Exception as err:
            print_error(err,'pydan_test1.refine_date')
    
    ##Now time to apply this function to the col=Dat.
    data.apply(colName='Dat', funcName='to_date', func=refine_date, 
               argNames=['Dat'])
    
    ##Now see how the Dat column looks like.
    data.head(5)
  
    ########Analyze the columns-types again.
    #for col in colTypes: print col,'   ', colTypes[col]    
    colTypes = data.analyze_columns()
    for col in colTypes: print col,'    ', colTypes[col]    
    dset = data.transform(resultTab='pydanTable', resultType=' TABLE ', 
                             colTypes=colTypes, returnQueryOnly=False)
    ##Drop the initial table.
    data.drop()
    data = None    
    return dset
      
def Main():
    dset = preAnalysis()
    
    dset.columns()
    dset.describe()
    #for c in dset.colTypes: print c, "   ",dset.colTypes[c]
    ##Now let is aggregate the information to year-month-week level.
    date = '2015-10-01'
    
    def month(date):
        return int(datetime.strptime(str(date),'%Y-%m-%d').date().strftime('%m'))
    def week(date):
        return int(datetime.strptime(str(date),'%Y-%m-%d').date().strftime('%W'))
    def year(date):
        return int(datetime.strptime(str(date),'%Y-%m-%d').date().strftime('%Y'))
    def uniqueWeek(weekOfYear, year):
        u= 100*int(year)+1*int(weekOfYear)
        print u
        return u
    dset.addColumn("myYear","TEXT",'to_year',year,argNames=['Dat'])
    dset.addColumn("myMonth","TEXT",'to_month',month,argNames=['Dat'])
    dset.addColumn("myWeek","TEXT",'to_week',week,argNames=['Dat'])
    dset.addColumn("unqWeek","NUMBER","to_unwk",uniqueWeek,argNames=['myWeek','myYear'])
    dt3 = dset.aggregate(resultTab='WEEKLY',aggOp=' AVG ', 
                        varNames=[
                                'NoofShares',
                                'ClosePrice',
                                'TotalTurnoverRs',
                                'LowPrice',
                                'HighPrice',
                                'SpreadCloseOpen'
                                ],
                        groupBy=['unqWeek'],
                        orderBy = ['unqWeek'],
                        conditions = ['1=1'],
                                   
                        )
    dt3.to_csv("C:\\Users\\SingAn22\\Desktop\\hcsvreader\\out.csv")
   

    gr = dt3.graph1D(
                    outfile='o.db',
                    varNameX='unqWeek',
                    varNameY='LowPrice', 
                    plotName='lPrice_Graph',
                    plotTitle='Low Price (Weekly)'
                      )
    gr2 = dt3.graph1D(
                    outfile='o.db',
                    varNameX='unqWeek',
                    varNameY='HighPrice', 
                    plotName='hPrice_Graph',
                    plotTitle='High Price (Weekly)'
                    )
    
    
    fig = plt.figure()
    ax  = fig.add_subplot(1,1,1)
    ax.set_axis_bgcolor('white')
    
    gr.get_cosmetics().lineWidth=2
    gr.get_cosmetics().color='blue'   
    gr2.get_cosmetics().color='red'
    gr.draw(ax)
    gr2.draw(ax)
    def make_label(value,pos):
        return str(int(value/100))+"-"+str(int(value-int(value/100)*100))
    
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(make_label))
    ax.grid(True)
    plt.show()
                  
    #dt = datetime.strptime(date, "%Y-%m-%d")
     
    #print dset.get_value('Dat',["Dat='"+(minDate)+"'"])
    #
    #d = csv.to_database(tabName='idiot', database='C:\\Users\\SingAn22\\Desktop\\hcsvreader\\tester.db',varTypes={'var':'NUMBER'})
    #d.describe()
    #print 'percentile: ',d.quantile('var',0.5)

Main()
