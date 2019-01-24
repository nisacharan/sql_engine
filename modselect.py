import sys
import csv
import glob
import os
import os.path
sys.path.insert(0,os.getcwd()+"/pyparsing-2.2.0")
import pyparsing

from pyparsing import *



#==PARSER========================================================================================================================================================
ParserElement.enablePackrat()
LPAR,RPAR,COMMA = map(Suppress,"(),")
select_stmt = Forward().setName("select statement")

# keywords
(UNION, ALL, AND, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
 CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET, OR) =  map(CaselessKeyword, """UNION, ALL, AND, INTERSECT, 
 EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, 
 DISTINCT, FROM, WHERE, GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET, OR""".replace(",","").split())
(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, 
 CURRENT_TIMESTAMP) = map(CaselessKeyword, """CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, 
 END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, 
 CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP""".replace(",","").split())
keyword = MatchFirst((UNION, ALL, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
 CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET, CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, 
 CURRENT_TIMESTAMP))

identifier = ~keyword + Word(alphas, alphanums+"_")
collation_name = identifier.copy()
column_name = identifier.copy()
column_alias = identifier.copy()
table_name = identifier.copy()
table_alias = identifier.copy()
index_name = identifier.copy()
function_name = identifier.copy()
parameter_name = identifier.copy()
database_name = identifier.copy()

# expression
expr = Forward().setName("expression")

integer = Regex(r"[+-]?\d+")
numeric_literal = Regex(r"\d+(\.\d*)?([eE][+-]?\d+)?")
string_literal = QuotedString("'")
blob_literal = Regex(r"[xX]'[0-9A-Fa-f]+'")
literal_value = ( numeric_literal | string_literal | blob_literal |
    NULL | CURRENT_TIME | CURRENT_DATE | CURRENT_TIMESTAMP )
bind_parameter = (
    Word("?",nums) |
    Combine(oneOf(": @ $") + parameter_name)
    )
type_name = oneOf("TEXT REAL INTEGER BLOB NULL")

expr_term = (
    CAST + LPAR + expr + AS + type_name + RPAR |
    EXISTS + LPAR + select_stmt + RPAR |
    function_name.setName("function_name") + LPAR + Optional("*" | delimitedList(expr)) + RPAR |
    literal_value |
    bind_parameter |
    Combine(identifier+('.'+identifier)*(0,2)).setName("ident")
    )

UNARY,BINARY,TERNARY=1,2,3
expr << infixNotation(expr_term,
    [
    (oneOf('- + ~') | NOT, UNARY, opAssoc.RIGHT),
    (ISNULL | NOTNULL | NOT + NULL, UNARY, opAssoc.LEFT),
    ('||', BINARY, opAssoc.LEFT),
    (oneOf('* / %'), BINARY, opAssoc.LEFT),
    (oneOf('+ -'), BINARY, opAssoc.LEFT),
    (oneOf('<< >> & |'), BINARY, opAssoc.LEFT),
    (oneOf('< <= > >='), BINARY, opAssoc.LEFT),
    (oneOf('= == != <>') | IS | IN | LIKE | GLOB | MATCH | REGEXP, BINARY, opAssoc.LEFT),
    ((BETWEEN,AND), TERNARY, opAssoc.LEFT),
    (IN + LPAR + Group(select_stmt | delimitedList(expr)) + RPAR, UNARY, opAssoc.LEFT),
    (AND, BINARY, opAssoc.LEFT),
    (OR, BINARY, opAssoc.LEFT),
    ])

compound_operator = (UNION + Optional(ALL) | INTERSECT | EXCEPT)
ordering_term = Group(expr('order_key') + Optional(COLLATE + collation_name('collate')) + Optional(ASC | DESC)('direction'))
join_constraint = Group(Optional(ON + expr | USING + LPAR + Group(delimitedList(column_name)) + RPAR))
join_op = COMMA | Group(Optional(NATURAL) + Optional(INNER | CROSS | LEFT + OUTER | LEFT | OUTER) + JOIN)
join_source = Forward()
single_source = ( (Group(database_name("database") + "." + table_name("table*")) | table_name("table*")) + 
                    Optional(Optional(AS) + table_alias("table_alias*")) +
                    Optional(INDEXED + BY + index_name("name") | NOT + INDEXED)("index") | 
                  (LPAR + select_stmt + RPAR + Optional(Optional(AS) + table_alias)) | 
                  (LPAR + join_source + RPAR) )
join_source << (Group(single_source + OneOrMore(join_op + single_source + join_constraint)) | 
                single_source)
result_column = "*" | table_name + "." + "*" | Group(expr + Optional(Optional(AS) + column_alias))
select_core = (SELECT + Optional(DISTINCT | ALL) + Group(delimitedList(result_column))("columns") +
                Optional(FROM + join_source("from*")) +
                Optional(WHERE + expr("where_expr")) +
                Optional(GROUP + BY + Group(delimitedList(ordering_term)("group_by_terms")) + 
                        Optional(HAVING + expr("having_expr"))))
select_stmt << (select_core + ZeroOrMore(compound_operator + select_core) +
                Optional(ORDER + BY + Group(delimitedList(ordering_term))("order_by_terms")) +
                Optional(LIMIT + (Group(expr + OFFSET + expr) | Group(expr + COMMA + expr) | expr)("limit")))
tests = sys.argv[1]
try:
    # print "-----------------------parsed query---------------------------"
    #print select_stmt.parseString(tests).dump()
    select_stmt.runTests(tests,printResults=True,fullDump=False)
    query= select_stmt.parseString(tests)
    queryListOfTokens = query.asList()
    # for i in queryListOfTokens:
    #     print i
    # print "--------------------------------------------------------------"

    # print queryListOfTokens[1][3]
    #print type(queryListOfTokens)
except ParseException, pe:
    print pe.msg

#==DEFINE AND POPULATE DB METHODS====================================================================================================================================================================


#==Globals and Flags===========================================================
dbFramework={} # [TableNames : [ColNames : Number of this col] ]
dbFramework["ansDbFromCp"] = {}
actualDb = {} # [TName : list of list of cols ]
colsOfTablesDb = {} #[Tname : list of cols]
starPresent = 0
distFlag = 0
whereFlag = 0
aggrFlag = 0
logicOps = ['=','>','<','<=','>=']
andOrOr = ["AND","OR"]
logicFlag = 0
andOrOrFlag = 0
liStar = ['*']
if "DISTINCT" in queryListOfTokens:
    distFlag = 1
elif liStar in queryListOfTokens:
    starPresent = 1
elif not distFlag:
    if len(queryListOfTokens) > 4:
        print type(queryListOfTokens[4])
        if "WHERE" in queryListOfTokens:
            whereFlag=1
        if queryListOfTokens[5][1] in logicOps:
            logicFlag = 1
            logicOperator = queryListOfTokens[5][1]
            logicOperands = []
            logicOperands.append(queryListOfTokens[5][0])
            logicOperands.append(queryListOfTokens[5][2])
        elif queryListOfTokens[5][1] in andOrOr:
            andOrOrFlag = 1
            andOrOrOperator = queryListOfTokens[5][1]
else:
    "distinct and where can't occur together [ not handled ]"


#==defines the data structures dbFra and colsOfTab===================
def definedb():
    metadata = open("metadata.txt","r")
    linesList = metadata.readlines()
    numOfLines = len(linesList)
    for i in range(numOfLines):
        curLine = linesList[i].rstrip("\r\n")
        if curLine == "<begin_table>":
            i+=1
            tempTableName = linesList[i].rstrip("\r\n")
            dbFramework[tempTableName] = {}
            colsOfTablesDb[tempTableName] = []
            i+=1
            j=0
            while(linesList[i].rstrip("\r\n") != "<end_table>"):
                curColName = linesList[i].rstrip("\r\n")
                dbFramework[tempTableName][curColName] = j
                colsOfTablesDb[tempTableName].append(curColName)
                j+=1
                i+=1
#==defines and populates actualDb from csv files=======================
def populatedb(listOfTables):
    for i in listOfTables:
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

#=====dumps into op csv==================================================
def dumpIntoOpFile(listToBeDumped,mode):
    opfile = open("output.csv", mode)
    wr = csv.writer(opfile)
    wr.writerow(listToBeDumped) 


#=========aggregate functions============================================
def findMax(aggrColName,curTname):
    colIndex = dbFramework[curTname][aggrColName]
    reqColAggr = []
    for i in range(1,len(actualDb[curTname])):
        reqColAggr.append(int(actualDb[curTname][i][colIndex]))
    print reqColAggr
    return max(reqColAggr)

def findMin(aggrColName,curTname):
    colIndex = dbFramework[curTname][aggrColName]
    reqColAggr = []
    for i in range(1,len(actualDb[curTname])):
        reqColAggr.append(int(actualDb[curTname][i][colIndex]))
    print reqColAggr
    return min(reqColAggr)

def findSum(aggrColName,curTname):
    colIndex = dbFramework[curTname][aggrColName]
    reqColAggr = []
    for i in range(1,len(actualDb[curTname])):
        reqColAggr.append(int(actualDb[curTname][i][colIndex]))
    print reqColAggr
    return sum(reqColAggr)

def findAvg(aggrColName,curTname):
    colIndex = dbFramework[curTname][aggrColName]
    reqColAggr = []
    for i in range(1,len(actualDb[curTname])):
        reqColAggr.append(int(actualDb[curTname][i][colIndex]))
    print reqColAggr
    return sum(reqColAggr)/float(len(reqColAggr))

def findDistinct(aggrColName,curTname):
    colIndex = dbFramework[curTname][aggrColName]
    reqColAggr = []
    for i in range(1,len(actualDb[curTname])):
        reqColAggr.append(int(actualDb[curTname][i][colIndex]))
    print reqColAggr
    myset = set(reqColAggr)
    return list(myset)


#====defines querytableList and whether star or distinct=========================
if distFlag:
    tableList = queryListOfTokens[4]
    queryTableList = []
else:
    tableList = queryListOfTokens[3] #this includes null tables
    queryTableList = [] #nulls filterd list of tables

if(isinstance(tableList,basestring)): #if 1 table it is stored as string else as a list with some [] in between
    queryTableList.append(tableList) #single table means appended directly as a string
else:
    for i in tableList:
        if len(i) != 0:
            queryTableList.append(i)



#==========CALL TO DB FORMATION===============================================================================================================================================================
definedb()
populatedb(queryTableList)    
#==========SELECT IMPLEMENTATION==============================================================================================================================================================
def handeStar():
    if len(queryTableList) == 1: 
        curTname = queryTableList[0]
        curColList = colsOfTablesDb[curTname]
        oprow1 = [] #Header of opfile
        cntr=0
        for i in curColList:
            if cntr == 0:
                oprow1.append("<"+curTname+"."+i)
            elif cntr == len(curColList)-1:
                oprow1.append(curTname+"."+i+">")
            else:
                oprow1.append(curTname+"."+i)
            cntr+=1
        dumpIntoOpFile(oprow1,'w')
        ansData = [[]]
        for i in range(len(actualDb[curTname])):
            if len(actualDb[curTname][i]) !=0:
                ansData.append(actualDb[curTname][i])
        dumpIntoOpFile(ansData,'a')
        print ansData
    else:
        #TODO [if equal col, natural join else cartesian product]
        print "[natural join logic here based on equal column]"

def handleDistinct(colNums,curTname):
    listOfRows = [[]]
    for i in range(1,len(actualDb[curTname])):
        tempList = []
        for j in colNums:
            tempList.append(actualDb[curTname][i][j])
        if tempList not in listOfRows:
            listOfRows.append(tempList)
    return listOfRows

def cartesianProduct(t1,t2):
    ans = [[]]
    for i in t1[1:]:
        for j in t2[1:]:
            ans.append(i+j)
    return ans

def checkCondition(val1,condition,val2):
        if condition == "=" :
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

def printOutput(listOfRows):
    for row in listOfRows:
        if row:
            mystring = ', '.join(map(str,row))
            print mystring




#===if no star and colnames given========================================
if not starPresent:
    curTname = queryTableList[0]
    askedCols = []
    if distFlag:
        queryListOfListOfColumns = queryListOfTokens[2]
    else:
        queryListOfListOfColumns = queryListOfTokens[1]
    for i in range(len(queryListOfListOfColumns)):
        if len(queryListOfListOfColumns[i])==1: #normal colums
            askedCols.append(queryListOfListOfColumns[i][0])
        else: #cols with aggregate functions
            askedCols.append(queryListOfListOfColumns[i][1])
            aggrFlag=1
    colNums = []  
    for i in askedCols:
        colNums.append(dbFramework[curTname][i])


    ansData = [[]]  
    if len(queryTableList) == 1: #single tables
        if aggrFlag:
            if queryListOfListOfColumns[0][0] == "max":
                maxval = findMax(queryListOfListOfColumns[0][1],curTname)
                print "max value is"
                print maxval
            elif queryListOfListOfColumns[0][0]=="min":
                minval = findMin(queryListOfListOfColumns[0][1],curTname)
                print "min value is"
                print minval
            elif queryListOfListOfColumns[0][0]=="avg":
                avgval = findAvg(queryListOfListOfColumns[0][1],curTname)
                print "avg value is"
                print("%.2f"%avgval)
            elif queryListOfListOfColumns[0][0]=="sum":
                sumval = findSum(queryListOfListOfColumns[0][1],curTname)
                print "sum value is"
                print sumval

        if distFlag:
            ansData = handleDistinct(colNums,curTname)
        else:
            for i in range(len(actualDb[curTname])):             
                tempRow = []
                if len(actualDb[curTname][i]):
                    for j in range(len(colNums)):
                        tempRow.append(actualDb[curTname][i][j])
                ansData.append(tempRow)
        printOutput(ansData)


    else:
        #TODO [if equal col, natural join else cartesian product]
        actualTableList = []
        for i in tableList:
            if(isinstance(i,basestring)):
                actualTableList.append(i)
        if len(actualTableList) == 2: #handling only join of 2 tables
            firstTableinCp =  actualDb[actualTableList[0]]
            SecondTableinCp = actualDb[actualTableList[1]]
            firstTableColsList = colsOfTablesDb[actualTableList[0]]
            secondTableColsList = colsOfTablesDb[actualTableList[1]]

            k=0
            for i in firstTableColsList:
                dbFramework["ansDbFromCp"][actualTableList[0]+"."+i] = k
                k+=1
            p=0
            for i in secondTableColsList:
                dbFramework["ansDbFromCp"][actualTableList[1]+"."+i] = k+p
                p+=1
            ansDbFromCp = cartesianProduct(firstTableinCp,SecondTableinCp)
        actualDb["ansDbFromCp"] = ansDbFromCp

        if whereFlag:
            if andOrOrFlag:
                # print queryListOfTokens[-1]
                whereCondListOfList = [[]]
                for i in range(len(queryListOfTokens[-1])):
                    if i%2 == 0:
                        whereCondListOfList.append(queryListOfTokens[-1][i])

                # print len(whereCondListOfList)
                colsOfWhere = []
                valsOfWhere = []
                opsOfWhere = []
                colsOfWhereIndices = []
                #         elif i in if i in colsOfTablesDb[actualTableList[1]]:
                #             colsOfWhereIndices.append(dbFramework[actualTableList[0]][i]+len(colsOfTablesDb[actualTableList[0]]))
                for rows in whereCondListOfList:
                    if rows:
                        colsOfWhere.append(rows[0])
                        opsOfWhere.append(rows[1])
                        valsOfWhere.append(rows[2])
                print colsOfWhere

                for i in colsOfWhere:
                    colsOfWhereIndices.append(dbFramework["ansDbFromCp"][i])
                print colsOfWhereIndices
                    
                cntr=0
                for i in range(1,len(actualDb["ansDbFromCp"])):
                    for j in range(1,len(colsOfWhereIndices)):             
                        if checkCondition(int(actualDb["ansDbFromCp"][i][colsOfWhereIndices[j]]),opsOfWhere[j],int(valsOfWhere[j])):
                            cntr+=1
                    if cntr==len(whereCondListOfList):
                        ansData.append(actualDb["ansDbFromCp"][i])
                        cntr=0
                tempAnsData = [[]]
                
                for i in range(1,len(ansData)):
                    tempRow=[]
                    for j in range(1,len(colNums)):
                        tempRow.append(ansData[i][j])
                    tempAnsData.append(tempRow)
                printOutput(tempAnsData)
        else:   
            ansData = ansDbFromCp


#===star is present========
elif starPresent: 
    if len(queryTableList) == 1:
        curTname = queryTableList[0]
        colsList = colsOfTablesDb[curTname]
        for i in colsList:
            print curTname+"."+i
        colNums = []
        for i in dbFramework[curTname]:
            colNums.append(dbFramework[curTname][i])
        ansData = [[]]
        if distFlag:
            ansData = handleDistinct(colNums,curTname)
        else:
            for i in range(len(actualDb[curTname])):             
                tempRow = []
                if len(actualDb[curTname][i]):
                    for j in range(len(colNums)):
                        tempRow.append(actualDb[curTname][i][j])
                ansData.append(tempRow)
        printOutput(ansData)

    else:
        if whereFlag:
            printOutput(actualDb["ansDbFromCp"])
        else:
            printOutput(actualDb["ansDbFromCp"])
