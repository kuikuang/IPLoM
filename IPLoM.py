from collections import Counter
import operator
import time
import copy

def step1(filePathRead='D:/data/test/HDFS_2k.log'):                         #按照每行词的个数来进行划分 具有相同词数的line被分到一组
    partitionOne={}
    i=0
    with open(filePathRead, 'r') as fr:
        for line in fr:
            i+=1
            listLine=line.split()
            #if len(listLine[0])!=10 or i%6!=0 :
                #continue
            tokenCount=len(listLine)
            if not tokenCount in partitionOne:
                partitionOne[tokenCount]=[listLine]
            else:
                partitionOne[tokenCount].append(listLine)
    return partitionOne


def step2(cin):
    dict={}
    partitionTwo={}
    for i in cin:
        dict=uniqueCount(i, cin[i])
        partitionTwo[i]=parByTokenPos(cin[i], dict)
    return partitionTwo


def step3(cin):
   partitionThree=[]
   l=[]                                                                #用来记录determineP1andP2的结果，如果是list，直接加入partitionThree，如果是两个pos，则进行后续操作
   for i in cin:
       if isinstance(cin[i][0][0],str):                                #第二步划分结果与未划分相同的情况，也就是第二步划分无效的情况，这个时候list是双重嵌套
           l=determineP1andP2(cin[i])
           if isinstance(l[0],int):                                    #即determineP1andP2返回的是原划分还是两个pos值
               par=processMap(l[0],l[1],cin[i],partitionThree)         #用p1,p2来确定映射关系如1-1，1-M.M-1,M-M等，然后对这些关系做出处理
               if par:                                                 #即经过处理后仍有剩余日志
                   partitionThree.append(par)
           else:
               partitionThree.append(cin[i])
       else:
           for j in cin[i]:                                            #这个时候list是3重嵌套
               l=determineP1andP2(j)
               if isinstance(l[0], int):
                   par=processMap(l[0],l[1],j,partitionThree)
                   if par:                                             #即经过处理后仍有剩余日志
                       partitionThree.append(par)
               else:
                   partitionThree.append(j)

   return partitionThree


def step4(cin):                                                        #最后一步 提取日志模板
    event_id=1
    pos=[]
    template=[]
    for i in cin:
        event_id_str=str(event_id)
        pos=findPosOfVar(i)                                            #这个函数返回i中变量的位置
        for k in i:                                                    #在每条日志后面加上事件ID
            k.append(event_id_str)
        l = copy.deepcopy(i[0])                                        #这里之所以要用深拷贝是因为如果采用浅拷贝 那么strcutLog中的这一条日志语句会因为下面的处理变成模版
        for j in pos:
            l[j]='*'                                                   #将变量用*代替
        template .append(l)
        event_id+=1
        l=[]
    filePathTemplateWrite = 'D:/data/test_result/template.txt'
    with open( filePathTemplateWrite,'w') as fw:
        for i in template:
            fw.write(" ".join(i))                                       #将list转为string
            fw.write("\n")
    fw.close()
    filePathStructLogWrite = 'D:/data/test_result/struct_log.txt'       #写结构化日志
    with open(filePathStructLogWrite, 'w') as log_wirte:
        for i in cin:
            for j in i:
                log_wirte.write(" ".join(j))                             # 将list转为string
                log_wirte.write("\n")
    log_wirte.close()


def uniqueCount(length,par):                                            #token position 以及对应的set of unique word 用于step2
    l=set()
    dict={}
    for i in range(length):
        for  j in par:
            l.add(j[i])
        dict[i]=findSetOfUniqueWord(l)
        l.clear()
    return dict


def findSetOfUniqueWord(l):                                              #每一列的uniue set of
    dict={}
    list=[]
    for i in l:
        if not i in dict:
            dict[i]=1
            list.append(i)
    dict.clear()
    del dict
    return list


def parByTokenPos(list,dict):                                             #确定用来进行step2的token position,并进行分割
    token=[]
    length=len(dict[0])
    pos=0
    res=[]
    for j in dict:
        temp=len(dict[j])
        if length>temp:
            pos=j
            length=temp
    l=dict[pos]

    if len(l)==1:
        return list
    temp = []                                                              #list这里要用初始化，不能用clear 因为用了clear之后你指向的内容也会清空
    for i in l:
        for j in list:
            if j[pos]==i:
                temp.append(j)
        res.append(temp)
        temp = []
    return res


def determineP1andP2(list):
    CT=0.34
    tokenCount=float(len(list[0]))
    if tokenCount>2:
        count1=float(getCount1(list))                                       #获得每个位置只有一个值的position的个数
        gc=count1/tokenCount
        if gc<CT:
            P1,P2=getMappingPosition(list)
        else:
            return list
    elif tokenCount==2:
        P1,P2=getMappingPosition(list)
    else:
        return list
    return P1,P2


def getCount1(list):                                                         #获得每个位置只有一个值的position的个数
    count=0
    judge=True
    l=len(list[0])
    m=len(list)
    if m==1 :
        return l
    for i in range(l):
        for j in range(m-1):
            if list[j][i]!=list[j+1][i]:
                judge=False
        if judge:
            count+=1
        else:
            judge=True
    return count


def getMappingPosition(l):                                                     #不是按照基数排列，是按照基数出现的次数进行排列。选取出现频率最高的作为mapping pos.因为就直觉上来说如果两个集合是满射，他们的元素个数肯定是相同的
    print("getMappingPosition")

    tokenCount=len(l[0])
    cardinality={}
    listOfCar=[]
    listOfPos=[]                                                               #记录 p1,p2的list
    if tokenCount==2:
        return 0,1
    elif tokenCount>2:
        dict=uniqueCount(tokenCount,l)
        for i in dict:
            a=len(dict[i])
            cardinality[i]=a
            if(a!=1):                                                          #other than value one，去除1之后 保证可以有两个position的证明来自gc<CT
                listOfCar.append(a)
        freq=Counter(listOfCar)
        k=list(freq.keys())[0]                                                 #取这个dict第一个元素即频率最高的元素 因为这个map的key是元素，value是频率，我们需要通过获得key来获取value

        freqOfCar=freq[k]                                                      #freqOfCar是元素对应的频率，k和k1是元素
        if freqOfCar>1:
            for i in cardinality:
                if cardinality[i]==k:
                    listOfPos.append(i)
                if len(listOfPos)==2:
                    break
        if freqOfCar==1:
            k1 = list(freq.keys())[1]
            for i in cardinality:
                if cardinality[i] == k:
                    listOfPos.append(i)
                    break
            for i in cardinality:
                if cardinality[i] ==k1:
                    listOfPos.append(i)
                    break
    else:
        pass
    return listOfPos[0], listOfPos[1]


def processMap(p1,p2,par,partitionThree):
    print("processMap")
    s1,s2=setOfWordByPos(p1,p2,par)
    S1=list(s1)
    S2=list(s2)
    lOfS1=countOfS(S1, par, p1)                                                 #这个函数用来计算s1中的每个词在par中出现的行数
    lOfS2=countOfS(S2,par,p2)                                                   #这个函数用来计算s2中的每个词在par中出现的行数
    tokenValues = []                                                            #tokenValues代表我们选的那一侧的token值 比如说我们选了1-M的Mside，那么tokenValues就为M所代表的M个token
    for i in range(len(S1)):
        print(len(S1))
        type,distance,word,listOfMside=determineMappingType(i,lOfS1,lOfS2)      #这里我们规定 0是1-1型，1是1-M型，2是M-1型，3是M-M型，word代表1-M或者M-1中的1在set S1或者set S2中的位置，同理，listOfMside代表M在set S1或者S2中的位置
        if type==0:
            splitPos=p1
            tokenValues.append(S1[i])
        elif type==1:                                                           #1-M类型
            splitRank=getRankPosition(distance,type)
            if splitRank==1:                                                    #在后面的划分中取1side
                splitPos=p1
                tokenValues.append(S1[i])
            else:                                                               #在后面的划分中取Mside
                splitPos=p2
                for j in listOfMside:
                    tokenValues.append(S2[j])
        elif type==2:                                                           #M-1类型
            splitRank = getRankPosition(distance,type)
            if splitRank == 2:                                                  #取1side
                splitPos = p2
                tokenValues.append(S2[word])
            else:                                                               #取Mside
                splitPos = p1
                for k in listOfMside:
                    tokenValues.append(S1[k])
        else:
            continue

        L1,L2=parBySplitPos(par,splitPos,tokenValues)                           #L1代表根据splitPos分出来的partition，L2代表partition剩下的,s1[i]代表token值
        tokenValues=[]
        if L1:
            partitionThree.append(L1)
        if not L2:                                                              #该par已经为空 也就是说整个partiton已经划分完毕
            return L2
        par=L2
    return par                                                                  #这一步的目的是为了判断经过所有划分后是否还有日志残留，如果有 加入partitionThree


def setOfWordByPos(p1,p2,par):
    print("setOfWordByPos")
    s1=set()
    s2=set()
    str1=''
    str2=''
    for i in par:
        str1 = i[p1]
        str2 = i[p2]
        s1.add(str1)
        s2.add(str2)
    return s1,s2


def determineMappingType(index,s1,s2):
    word=s1[index]                                                                #我们进行处理的token的出现行数
    for i in s2:
        if operator.eq( word,i):                                                  #判断是否为1-1型
            return 0,0,[],[]
    judge,distance,word,listOfMSide=deter1_M( word,s2)
    if judge:                                                                     #判断是否为1-M型
        return 1,distance,index,listOfMSide                                       #这里的distance是在getRankPosition中要求的，因为在deter函数中比较方便求，所以就提前算出来，传到getRankPosition中
    judge,distance,listOfMSide1,word1=deterM_1( word,s1,s2)
    if judge:                                                                     #判断是否为M-1型
        return 2,distance,word1,listOfMSide1
    return 3,0,[],[]                                                              #以上都不是，则为M-M


def countOfS(s,par,p):
    print("countOfS")
    l=[]
    temp=[]
    length=len(par)
    if len(s)==1:
        a=s[0]
        for j in range(length):
            if par[j][p]==a:
                l.append(j)
        return l
    for i in s:
        for j in range(length):
            if par[j][p]==i:
                temp.append(j)
        l.append(temp)
        temp=[]
    return l


def deter1_M(s1,s2):
    wordOf1Side=s1
    listOfMSide = []
    if len(s1)==1:                                                                 #它只出现一行不可能是1-M类型
        return False,0,wordOf1Side,listOfMSide
    l=[]
    for i in s1:
        for j in range(len(s2)):
            if s2[j].count(i)==1:
                l.append(j)
    if l.count(l[0])==len(l):                                                      #这种情况就是s1的这个词对应的s2的词为相同的，就不可能是1-M
        return False,0,wordOf1Side,listOfMSide
    l=list(set(l))                                                                 #对list进行去重
    count=0
    for i in l:                                                                    #虽然s1对应的s2的词不为相同，但不能保证就是1-M，还需要保证出现的行数完全相同
        count+=len(s2[i])
    if count==len(s1):
        count=float(count)
        distance=len(l)/count
        return True,distance,s1,l
    return False,0,wordOf1Side,listOfMSide


def deterM_1(word,s1,s2):
    l = []
    for i in word:
        for j in range(len(s2)):
            if s2[j].count(i) == 1:
                l.append(j)
    if not l.count(l[0]) == len(l):                                                # 这种情况就是s1的这个词对应的s2的词为相同的，才有可能是M-1,如果不是这种情况，就不可能是M-1
        return False,0,[],[]
    judge,distance,word,listOfMSide=deter1_M(s2[l[0]],s1)
    if judge:                                                                      #所以确定是否为M-1的思路就是 我们先确定这个1 然后倒过来确定是否为1-M
        return True,distance,listOfMSide,l[0]
    return False,0,[],[]


def getRankPosition(distance,type):
    lowerBound=0.1
    upperBound=0.9
    if distance<=lowerBound:
        if type==1:                                                                 #为1-M类型
            splitRank=2
        else:                                                                       #为M-1类型
            splitRank=1
    elif distance>=upperBound:
        if type==1:
            splitRank=1
        else:
            splitRank=2
    else:
        if type==1:
            splitRank=1
        else:
            splitRank=2

    return splitRank

def parBySplitPos(par,splitPos,tokenValues):                                       #根据我们确定的SplitPos以及对应的token来对par进行划分
    print("parBySplitPos")
    L1=[]
    L2=[]
    for i in par:
        judge=True
        for j in tokenValues:
            if i[splitPos]==j:
                L1.append(i)
                judge=False
                break
        if judge:
            L2.append(i)
        print('for in parBySplitPos')
    return L1,L2

def findPosOfVar(par):                                                             #找到变量的位置
    length=len(par[0])
    judge=set()
    l=[]
    for i in range(length):
        for j in par:
            judge.add(j[i])
        if len(judge)!=1:
            l.append(i)
        judge.clear()
    return l

def testFunc():
    start=time.time()
    pOne=step1()
    pTwo=step2(pOne)
    pThree=step3(pTwo)
    step4(pThree)
    end=time.time()
    print(end-start)
testFunc()

