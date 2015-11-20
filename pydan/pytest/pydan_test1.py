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


def preAnalysis():
    
#   ##Initialize the csv reader
    csv = pycsv_reader("C:\\Users\\SingAn22\\Desktop\\hcsvreader\\533098.csv")
    ##Load the data into sqlite database, obtain pydset object.
    data = csv.to_database(tabName='pydanTable',database ='C:\\Users\\SingAn22\\Desktop\\hcsvreader\\dset.db')
    ##Print the column names from data 
    data.columns()
    ##At the moment every element is a string, so analyze what datatypes can be associated with each string 
    data.analyze_columns()  #Missing values are represented as empty strings, and they dont effect this operation.  
    ##Now print the columns again.
    data.columns()                                
     
preAnalysis()