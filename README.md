#pyDAN : TAKING DATA TO ALGORITHMS
==================================
##What is it?
----------------
The pyDAN is an ongoing to attempt to make the power of SQL analytics available to data scientists and statisticians and minimize dependence over SLQ engineer. The idea is to use all the tools at disposal namely SQL, Python, Java and Cython in one single framework and provide user with an unprecendented ability to pull, clean merge, munge and preanalyze data from multiple sources and stored across several platforms.

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
#How do I start?
------------------
The pyDAN project is still in infancy. The devel version can be obtained from this repository but there is no guarantee that it will work. A more stable and tested beta version can be <b>obtained on request from the author</b>.
