<h1><color='blue'><b><i>pyDAN</i> : TAKING DATA TO ALGORITHMS</b></color></h1> 
<pre>Author (contact): Anil Pratap Singh (singh.ap79@gm*l*.*om)</pre>
-----------------------------------------------
<table><tr>
<td><img align="middle" src=https://github.com/AnilSingh79/pydan/blob/master/pydan_header.PNG/></td>
<td><img align="middle" src=https://github.com/AnilSingh79/pydan/blob/master/pyDan.png/></td>
</tr></table>
===========================================================================

##What is it?
----------------
The pyDAN is an ongoing attempt to make the power of SQL analytics available to data scientists and statisticians and minimize dependence over database engineer. The idea is to use all the tools at disposal namely SQL, Python, Java and Cython in one single framework and provide user with an unprecendented ability to pull, clean merge, munge and preanalyze data from multiple sources and stored across several platforms.

##How do we do it?
-------------------
The SQL standard a high level layer providing uniform (okay nearly uniform) interface to a large number of  the database management systems. We propose to put up and additional layer (mostly in python) to relieve user from creating SQL (or most of it) code. 
<table>
<tr>
<td><img align="middle" src="https://github.com/AnilSingh79/pydan/blob/master/pyDan1.PNG"/></td>
<td>
<ol>
  <code><pre>
  ##Set the data source
  csv = pycsv_reader("mycsvfile.csv")
  remote = sql_reader("myurl","myuname","mypasswd")
  
  ##Read into sqlite scratch area
  dset1 = csv.to_database(tabName="mycsvtab",database="mysqlite.db")
  dset2 = csv.to_database(tabName="myremotetab",database="mysqlite.db"
  
  ##Let us do a join
  data = dset1.join(resultTab = "mymergedtab",d2=dset2, 
                  varNames1=['*'], 
                  varNames2=['var2'], 
                  on=["csv_var1=sql_var2"]
                  )
                  
  ##Apply my business logic to var1, var2 and add result in var_X
  data.addColumn("var_X","number","business_logic1",
                 bizLogicFunc,argNames=['var1','var2'])
  
  ##Dump the results into a csv file
  data.to_csv("myresultdata.csv")
</td>
</table>
For every action requested on data, pyDAN generate appropriate SQL under the hood and executes it using pysqlite library. Each time a track is kept for the result tables (if any), and they too become available in program for futher manipulation.

##What do I need to learn ?
-----------------------------
If you have beginner's skills at python and SQL, then its all you need to get started. Just know that in heart of pyDAN is the <code>pydset</code> class, which provides a pipe to an underlying tables in scratch sqlite. The <code>pydset</code> allows user to request maniputlations/queries on the data.
<table>
<tr>
<td>
<pre>
pydset.subset
pydset.join
pydset.union
pydset.unpivot
pydset.apply
pydset.aggregate
</pre>
</td>
<td>
<pre>
pydset.describe
pydset.sum/total/count
pydset.head/tail/show
pydset.mean/std/quantile/median
pydset.get1/get2
</pre>
</td>
<td>
<pre>
ptdset.skeleton
pydset.to_numpy
pydset.to_csv
pydset.to_database
pydset.hist1D
pydset.graph1D
</pre>
</td>
</tr>
<tr>Table 1: A part of pydset interface.</tr>
</table>
##How do I start?
------------------
The pyDAN project is still in infancy. The devel version can be obtained from this repository but there is no guarantee that it will work. A more stable and tested beta version can be <b>obtained on request from the author</b>. The beta version is supplied as a single python source file, and can be made available in your analysis by doing:
<code><pre>
import sys
sys.path.insert(0,"C:\Users\SingAn22\Desktop\PYDAN\Beta")
from pydan import *
</pre></code>

##What can I expect in future?
------------------------------
We are working on several channels
<ul>
 <li> Enhancement possible formats for data input. 
 <li> Enhancement of analytics library.
 <li> Enrichment of the SQL engine to automate generation of complex statements.
 <li> Enrichment of remote data processing abilities.
 <li> Enrichment of matplotlib interface for visualization.
</ul>


##How can I contribute?
----------------------
Well the first obvious step is to request a beta version and start using it. And then we can direct you to one of the several open lines of development at pyDAN.


##Disclaimer
-----------------
The pyDAN is a private project and yet not released for general consumption. We can not guarantee support of back-version compatibility at the present moment but we do encourage people to start using the package as it matures into a release version.

<br><br><br>

#Tutorial 1: Data from Stock Market
##Introduction
We use a file from Bombay Stock Exchange containing 3 years worth of daily listings for a particular stock. Using pyDAN we will demonstrate how to build a simple analytic routine. Here are some visualizations from analytics:
<table>
<tr>
<td><img align="middle" src="https://github.com/AnilSingh79/pydan/blob/master/figure_2.png"/></td>
<td><img align="middle" src="https://github.com/AnilSingh79/pydan/blob/master/figure_1.png"/></td>
<td>
</table>

## A Walkthrough
<ol>
<li> Libraries
<code><pre>
  from pydan.pycsv import pycsv_reader
  from pydan.pytools import month_enum, print_error,clean_string
  from pypaint.pygraph import TGraph

  from matplotlib import pyplot as plt
  from matplotlib import ticker
  from datetime import datetime
</pre></code>
<li> Load data into SQLITE scratchpad (data ingestion)
<code><pre>
  ##Register the source csv
  csv = pycsv_reader("fullPathTo\\533098.csv")
  
  ##Load the data into sqlite database, obtain pydset object.
  data = csv.to_database(tabName='pydanTable_prelim',database ='fullPathTo\\dset.db')
</pre></code>

<li> Change the Date format (String Level Manipulation)
<code><pre>
  #weave business requirements into a python function
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

  #Apply the business requirement to one of the columns 
  data.apply(colName='Dat', funcName='to_date', func=refine_date, argNames=['Dat'])
</pre></code>

<li> Analyze data in each column and set types (Automated Data Recognition)
<code><pre>
  #Analyze the data in columns for numericity
  colTypes = data.analyze_columns()
  
  #transform columns to their proper datatypes.
  dset = data.transform(resultTab='pydanTable',colTypes=colTypes)
</pre></code>

<li> Collapse the daily data to weekly level. 
<code><pre>
 #Creat logic for a unique marker for year-week combo
 def unqWeekMarker(date):
    year = int(datetime.strptime(str(date),'%Y-%m-%d').date().strftime('%Y'))
    week =  int(datetime.strptime(str(date),'%Y-%m-%d').date().strftime('%W'))
    u= 100*int(year)+1*int(week)
    
 #Time to add this new column to the dset
 dset.addColumn("unqWeek","NUMBER","to_unwk",uniqueWeek,argNames=['date'])

 #Now is the time to collapse (weekly averages)
 dt3 = dset.aggregate(resultTab='WEEKLY',aggOp=' AVG ', 
        varNames=['NoofShares','ClosePrice','TotalTurnoverRs','LowPrice','HighPrice','SpreadCloseOpen'],
        groupBy=['unqWeek'],
        orderBy = ['unqWeek'],
        conditions = ['1=1']
      )

</pre></code>

<li> Simple Visualization
<code><pre>
  #Graph low price against week-id
  gr = dt3.graph1D(
        outfile='o.db',
        varNameX='unqWeek',
        varNameY='LowPrice', 
        plotName='lPrice_Graph',
        plotTitle='Low Price (Weekly)'
      )
      
  #Graph hi price against week-id
  gr2 = dt3.graph1D(
        outfile='o.db',
        varNameX='unqWeek',
        varNameY='HighPrice', 
        plotName='hPrice_Graph',
        plotTitle='High Price (Weekly)'
      )
  
  #Create the canvas
  fig = plt.figure(facecolor='white')
  ax  = fig.add_subplot(1,1,1)
  gr.draw(ax)
  gr2.draw(ax)
  </pre></code>
  
  <li> Cosmetic Settings
  <code><pre>
  #Logic to turn ugly weekId (201245) into beautiful market 2012-45
  def make_label(value,pos):
    return str(int(value/100))+"-"+str(int(value-int(value/100)*100))
  
  #Set xaxis labels using this logic
  ax.xaxis.set_major_formatter(ticker.FuncFormatter(make_label))
  
  #Add a grid
  ax.grid(True)
  </pre></code>

  <li> Finally
  <code><pre>
  plt.show()
  </pre></code>

</ol>

</pre></code>
  



