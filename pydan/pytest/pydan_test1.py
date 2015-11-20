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

def preAnalysis():
    '''
     Most of these things should be done, interactively. 
     They give a flavor of what data looks like.
    '''
    
#   ##Initialize the csv reader
    csv = pycsv_reader("C:\\Users\\SingAn22\\Desktop\\hcsvreader\\533098.csv")
    ##Load the data into sqlite database, obtain pydset object.
    data = csv.to_database(tabName='pydanTable_prelim',database ='C:\\Users\\SingAn22\\Desktop\\hcsvreader\\dset.db')
    ##Print the column names from data 
    data.columns()
    ##At the moment every element is a string, so analyze what datatypes can be associated with each string
    #Missing values are represented as empty strings, and they dont effect this operation.
    colTypes = data.analyze_columns()   
    for col in colTypes: print col,'   ', colTypes[col]                                  
    ##Let us print first few lines close to top... 
   
    ##We saw that 'Dat' column is not in a format recognised by sqlite. So we need to fix it.     
    #####1. Come up with a format fixing function.
    #####2. Update the Dat column by applying fixing function to each element. 
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
    data.apply(colName='Dat', funcName='to_date', func=refine_date, numparams=1) 
    ##Now see how the Dat column looks like.
    data.head(5)
    ##Analyze the columns-types again.
    colTypes = data.analyze_columns()
    for col in colTypes: print col,'    ', colTypes[col]
    
    dset = data.transform(resultTab=' pydanTable ', resultType=' TABLE ', colTypes=colTypes, returnQueryOnly=False)
    data.drop()
    data = None    
    return dset
        
def Main():
    dset = preAnalysis()
    dset.columns()

Main()