import sys
import csv
import os
from collections import *
from pyparsing import *
from parser import *


try:
	queryString = sys.argv[1]
except IndexError,ie:
	print "===ERROR==="
	print ie
	exit()

tokensList = parser(queryString)


#===GLOBALS====
actualDb = OrderedDict()
dbFramework = {}
dbFramework["Joint"] = OrderedDict()
colsOfTablesDb = OrderedDict()
queryTables = []
ansRows = []
aggrCols=[]
aggrFuncs=[]
distinctFlag = False
queryColumns = []
starFlag = False
whereFlag = False
aggrList = ["max","min","avg","sum","MAX","MIN","AVG","SUM"]
aggrFlag = False
#==extracts table names from query
if isinstance(tokensList[tokensList.index("FROM")+1],basestring):
	queryTables.append(tokensList[tokensList.index("FROM")+1])
else:
	for i in tokensList[tokensList.index("FROM")+1]:
		if i:
			queryTables.append(i)

#==extracts where list
if "WHERE" in tokensList:
	whereFlag = True
	whereList = tokensList[tokensList.index("WHERE")+1]
	cntr=0
	for i in whereList:
		if isinstance(i,basestring):
			cntr+=1
	if cntr == len(whereList):
		pass

#===distinct part===
if "DISTINCT" in tokensList:
	distinctFlag = True

starL = ['*']
if starL in tokensList:
	starFlag = True



if distinctFlag:
	queryColumns = tokensList[tokensList.index("DISTINCT")+1]
else:
	queryColumns = tokensList[tokensList.index("SELECT")+1]

	if queryColumns[0][0] in aggrList:
		aggrFlag = True

#==defines global db's====
def definedb():
    metadata = open("metadata.txt","r")
    linesList = metadata.readlines()
    numOfLines = len(linesList)
    for i in range(numOfLines):
        curLine = linesList[i].rstrip("\r\n")
        if curLine == "<begin_table>":
            i+=1
            tempTableName = linesList[i].rstrip("\r\n")
            dbFramework[tempTableName] = OrderedDict()
            colsOfTablesDb[tempTableName] = []
            i+=1
            j=0
            while(linesList[i].rstrip("\r\n") != "<end_table>"):
                curColName = linesList[i].rstrip("\r\n")
                dbFramework[tempTableName][curColName] = j
                colsOfTablesDb[tempTableName].append(curColName)
                j+=1
                i+=1

#==checks if all elem in list are strings
def isAllStrings(li):
	cntr=0
	for i in li:
		if isinstance(i,basestring):
			cntr+=1
	if cntr==len(li):
		return True
	else:
		return False

#==populates actualDb
def populatedb(queryTables):
    for i in queryTables:
        tempList=[[]]
        fname = i+".csv"
        if os.path.isfile(fname):
            with open(fname,"r") as fp:         
                csvRdrObj = csv.reader(fp)
                for row in csvRdrObj:
                    tempList.append(row)
                actualDb[i]=tempList
        else:
            print "------------ERROR-----------------"
            print "      Table doesn't exist"
            exit() 

#==cart prod of two matrices
def cartProd(t1,t2):
    ans = [[]]
    for i in t1[1:]:
        for j in t2[1:]:
            ans.append(i+j)
    return ans

#==joins n tables iteratively
def joinTables(queryTables):
	if len(queryTables) == 1:
		return actualDb[queryTables[0]]

	ans = actualDb[queryTables[0]]
	for i in range(1,len(queryTables)):
		ans = cartProd(ans,actualDb[queryTables[i]])
	return ans

#==evaluates condition
def checkCondition(val1,condition,val2):
        if condition == "==" :
            if val1 == val2 :
                return True
        elif condition == ">=" :
            if val1 >= val2 :
                return True
        elif condition == "<=" :
            if val1 <= val2 :
                return True
        elif condition == ">" :
            if val1 > val2:
                return True
        elif condition == "<" :
            if val1 < val2:
                return True
        elif condition == "!=" :
            if val1 != val2 :
                return True
        else :
            return False

#==evaluates tiplets and returns 1shots
def evalTriplet(whereList):
	if '.' not in whereList[0]: #if there is an expr like a >20 it is made like t1.a > 20
		for i in colsOfTablesDb:
			if whereList[0] in colsOfTablesDb[i] and i in queryTables:
				whereList[0]=i+"."+whereList[0]

	# print dbFramework["Joint"]
	c1 = dbFramework["Joint"][whereList[0]]

	bothAreTableFlag = False
	if not whereList[2].isdigit():
		bothAreTableFlag = True
		c2=dbFramework["Joint"][whereList[2]]

	else:
		c2=int(whereList[2])

	if whereList[1] is "=":
		whereList[1] = "=="
	oper = whereList[1]


	rowHits = []
	for row in actualDb["Joint"][1:]:
		if bothAreTableFlag:
			if checkCondition(int(row[c1]),oper,int(row[c2])):
				rowHits.append(1)
			else:
				rowHits.append(0)
		else:
			if checkCondition(int(row[c1]),oper,c2):
				rowHits.append(1)
			else:
				rowHits.append(0)			
	return rowHits




#==evaluates sets and return 1 shots
def evalSets(l1,op,l2):
	ans = []

	for i in range(len(l1)):
		if op == "OR":
			ans.append(l1[i] or l2[i])
		elif op == "AND":
			ans.append(l1[i] and l2[i])
	return ans
#==unwraps where and brings us expressions in a list
def unwrapWhere(whereList):
	if isAllStrings(whereList):
		whereList = evalTriplet(whereList)
		return whereList
	else:
		l1=unwrapWhere(whereList[0])
		op=whereList[1]
		l2=unwrapWhere(whereList[2])
		whereList = evalSets(l1,op,l2)
		return whereList


#==adds join table in dbf==
def addJoinInDBF(queryTables):
	allCols = []
	for i in queryTables:
		trow = []
		for j in colsOfTablesDb[i]:
			trow.append(i+"."+j)
		allCols=allCols+trow
	cntr=0
	for i in allCols:
		dbFramework["Joint"][i]=cntr
		cntr+=1

#==prints pretty output====
def printOutput(listOfRows):
	for row in listOfRows:
		if row:
			mystring = ', '.join(map(str,row))
			print mystring


#==filter rows==
def filterRows(ansRows):
	ansDb=[[]]
	# print len(ansRows)
	# print len(actualDb["Joint"])
	try:
		for i in range(1,len(actualDb["Joint"])):
			pass
			# print ansRows[i]
			if ansRows[i-1] == 1 :
				ansDb.append(actualDb["Joint"][i])
	except TypeError,te:
		print "No Results Found"
	return ansDb

#==fetch all columns
def fetchAllColumns():
	allCols = []
	for i in queryTables:
		for j in colsOfTablesDb[i]:
			allCols.append(i+"."+j)
	return allCols

# #==aggregate values are calculated for a given column
# def aggrVal(col,func):

# 	if fun is "max":
# 		print "TTTTT"

#==evaluates aggregate functions
def evalAggrFuncs(aggrFuncs,aggrColNums):
	aggrAns=[]
	anss=[[]]
	for i in aggrColNums:
		colNum=i
		reqColAggr=[]
		for j in range(1,len(actualDb["Joint"])):
			reqColAggr.append(int(actualDb["Joint"][j][i]))
		anss.append(reqColAggr)

	for i in range(len(aggrFuncs)):
		if aggrFuncs[i] == "max":
			aggrAns.append(max(anss[i+1]))
		elif aggrFuncs[i] == "min":
			aggrAns.append(min(anss[i+1]))
		elif aggrFuncs[i] == "sum":
			aggrAns.append(sum(anss[i+1]))
		elif aggrFuncs[i] == "avg":
			aggrAns.append(sum(anss[i+1])/float(len(anss[i+1])))

	return aggrAns


	# print actualDb["Joint"]
	# res=[[]]
	# for i in actualDb["Joint"][1:]:
	# 	reqCol=[]
	# 	for j in aggrColNums:
	# 		reqCol.append(int(i[j]))
	# 	res.append(reqCol)
	# print res
	# colsList=zip(*res)
	# colsList=[[]]
	# # for i in zip(*res):
	# # 	colsList.append((list(i)))
	# print colsList
	# colsList=[[]]

	# for i in res:
	# 	tempCol=[]
	# 	for j in range(len(res[0])):
	# 		tempCol.append(i[j])
	# 	colsList.append(tempCol)
	# print colsList



#==filters columns
def filterColumns(queryColumns,ansDbAllCols):
	colNums = []
	mainAns=[[]]

	if starFlag:
		for i in queryColumns:
			colNums.append(dbFramework["Joint"][i])
	elif aggrFlag:
		for i in queryColumns:
			colNums.append(dbFramework["Joint"][i])

	
	else:
		colNames=[]
		colNamesExt = []
		for i in queryColumns:
			if i:
				mystring = ', '.join(map(str,i))
				colNames.append(mystring)
		# print colNames
		# print colsOfTablesDb
		for j in colNames:
			for i in colsOfTablesDb:
				if i in queryTables:				
					if j in colsOfTablesDb[i]:
						# if cnt is 0:
						colNamesExt.append(i+"."+j)
							# cnt+=1
		print ', '.join(colNamesExt)
		for i in colNamesExt:
			colNums.append(dbFramework["Joint"][i])


	if aggrFlag:
		aggrColNames=[]
		for i in aggrCols:
			for j in queryTables:
				if i in colsOfTablesDb[j]:
					aggrColNames.append(j+"."+i)
		print ', '.join(aggrColNames)

		aggrColNums = []
		for i in aggrColNames:
			aggrColNums.append(dbFramework["Joint"][i])
		mainAns[0]= evalAggrFuncs(aggrFuncs,aggrColNums)



	else:
		for row in ansDbAllCols[1:]:
			temprow=[]
			for j in colNums:
				temprow.append(row[j])
			if distinctFlag:
				if temprow not in mainAns:
					mainAns.append(temprow)
			else:
				mainAns.append(temprow)
	return mainAns


#==function calling area===
definedb()
populatedb(queryTables)
ans = joinTables(queryTables)
actualDb["Joint"] = ans
addJoinInDBF(queryTables)


ansDbAllCols = ans
if aggrFlag:


	for i in queryColumns:
		aggrCols.append(i[1])
		aggrFuncs.append(i[0])
	queryColumns = aggrCols
	queryColumns = fetchAllColumns()

if whereFlag:
	ansRows = unwrapWhere(whereList)
	ansDbAllCols = filterRows(ansRows)

if starFlag:
	queryColumns = fetchAllColumns()
	print ', '.join(queryColumns)

ansDb = filterColumns(queryColumns,ansDbAllCols)
printOutput(ansDb)



