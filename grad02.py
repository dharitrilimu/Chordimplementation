'''
Created on Apr. 15, 2019

@author: dharitri
'''
import socket
import random
import string
import time
filestore={}
peer0files=[]
peer1files=[]
peer2files=[]
peer3files=[]
peer4files=[]
keystores={}
filessentpeer0=[]
filessentpeer1=[]
filessentpeer2=[]
filessentpeer3=[]
filessentpeer4=[]
class node:
    #Class that defines the current node
    def __init__(self,phname,phport,nname,nport,shname,shport,id):
        self.pn = phname
        self.pp = phport
        self.nn = nname
        self.np = nport
        self.sn = shname
        self.sp = shport
        self.id = id
    def printNode(self):
        print("Node Id : "+str(self.id))
        print("Previous node : "+self.pn)
        print("Current node : "+self.nn)
        print("Successor node : "+self.sn)    
    
      
    def listensocket(self):
        global sock1        
        sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock1.bind((self.nn, self.np))
        sock1.listen(10)
        while True:
            c, addr = sock1.accept()    
            recevdata=c.recv(1024).decode()
            nodevalues=recevdata.split('|')
            if nodevalues[0]=='JOIN':
                print("JOIN")
              
            elif nodevalues[0]=='UPDATEPRED':
                print("UPDATEPRED")
                self.sn=nodevalues[1]
                self.printNode()
                c.sendall("completed".encode())
                
            elif nodevalues[0]=='UPDATESUCC':
                print("UPDATESUCC")
                self.pn=nodevalues[1]
                self.printNode()
                c.sendall("completed".encode())
            elif nodevalues[0]=='FINDPREDECESSOR':
                c.sendall(str(self.pn).encode())
            elif nodevalues[0]=='FINDSUCCESSOR':
                c.sendall(str(self.sn).encode())
            elif nodevalues[0]=='STOREOBJ':
                
                frompeerid=int(int(nodevalues[3]))
                if frompeerid==0:
                    peer0files.append(nodevalues[2])
                    filestore.update({0:peer0files})
                    print("file stored in grad02!!!Request generator grad01")
                elif frompeerid==1:
                    peer1files.append(nodevalues[2])
                    filestore.update({1:peer1files})
                    print("file stored in grad02!!!Request generator grad02")
                elif frompeerid==2:
                    peer2files.append(nodevalues[2])
                    filestore.update({2:peer2files})
                    print("file stored in grad02!!!Request generator grad03")
                elif frompeerid==3:
                    peer3files.append(nodevalues[2])
                    filestore.update({3:peer3files})
                    print("file stored in grad02!!!Request generator grad04")
                else:
                    peer4files.append(nodevalues[2])
                    filestore.update({4:peer4files})
                    print("file stored in grad02!!!Request generator grad05")
                print("file stored in peer2 as tuple")                   
                print(filestore[2])           
                c.sendall(("file "+nodevalues[2] +" is stored successfully in grad02").encode())          
            else:
                print("no request")
roothost='10.0.130.21'                 
hostname='10.0.130.22'
hostport=12111
def sendRequest(host,port,data):
        socket1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket1.connect((host,port))
        socket1.sendall(data.encode())
        response = socket1.recv(1024).decode()
        return response
def updatepredecessor(node,predecessor):
    predupdaterequest='UPDATEPRED|'+node
    response=sendRequest(predecessor, hostport, predupdaterequest)
    return response
def updatesuccessor(node,successor):
    print("inside update successor method")
    succupdaterequest='UPDATESUCC|'+node
    response=sendRequest(successor, hostport, succupdaterequest)
    return response
def predecessor():
    requeststring='FINDPREDECESSOR'
    predecessor=sendRequest(hostname, hostport, requeststring)
    print(predecessor)
    return predecessor
def successor():
    requeststring='FINDSUCCESSOR'
    successor=sendRequest(hostname, hostport, requeststring)
    print(successor)
    return successor
#removing id
def updateroot():
    requeststring='UPDATECHORD|'+str(2)
    response=sendRequest(roothost, hostport, requeststring)
    return response      
def leaveChord():
    print("hello from leavechord")
    succ=successor()
    pred=predecessor()
    updatepredecessor(succ, pred)
    updatesuccessor(pred, succ)
    response=updateroot()
    node3=node('',0,hostname,hostport,'',0,1)
    node3.printNode()
    print(response)
def fileRandomStore():
    starttime=time.time()
    peerid=1
    print("filerandomstore")
    stringLength=4
    letters = string.ascii_uppercase 
    filestring= ''.join(random.choice(letters) for i in range(stringLength))
    print(filestring)
    total=0
    for c in filestring:
              
        number=ord(c)
        print(number)
        total=total+number
        
    fileposition=total%5
    
    print(fileposition)
    requeststring1= 'STOREOBJ|'+ str(fileposition)+'|'+filestring+'|'+str(peerid)
    
    filestoringpeer=sendRequest(roothost, hostport, requeststring1)
    if filestoringpeer=='STOREDINROOT':
        print(filestoringpeer)
        filessentpeer0.append(filestring)
        print(filestoringpeer)
        keystores.update({0:filessentpeer0})
        print("dictionary")
        print(keystores[0])
    else:
        
        if filestoringpeer=='10.0.130.22':
            filessentpeer1.append(filestring)
            keystores.update({1:filessentpeer1})
            
            print(keystores[1])
        elif filestoringpeer=='10.0.130.23':
            filessentpeer2.append(filestring)
            keystores.update({2:filessentpeer2})
            
            print(keystores[2])
        elif filestoringpeer=='10.0.130.24':
            filessentpeer3.append(filestring)
            keystores.update({3:filessentpeer3})
            
            print(keystores[3])
        else:
            filessentpeer4.append(filestring)
            keystores.update({4:filessentpeer4})
            
            print(keystores[4])
        requeststring= 'STOREOBJ|'+ str(fileposition)+'|'+filestring+'|'+str(peerid)
            
        response=sendRequest(filestoringpeer, hostport, requeststring)
        print(response)
        endtime=time.time()
        timetaken= endtime-starttime
        print("Total processing time to store files: "+str(timetaken))
def fileStore():
    starttime=time.time()
    peerid=2
    print("filestore")
    listoffiles=['ABXE','CFJZ','GEHI','XUJG','HUIJ','XUVA','BHUI']
    total=0
    for filestring in listoffiles:
        for c in filestring:
            number=ord(c)
            print(number)
            total=total+number
        
        fileposition=total%5
        requeststring= 'STOREOBJ|'+ str(fileposition)+'|'+filestring+'|'+str(peerid)
        starttime=time.time()
        filestoringpeer=sendRequest(roothost, hostport, requeststring)
        print(fileposition)
        
    if filestoringpeer=='STOREDINROOT':
        print(filestoringpeer)
        filessentpeer0.append(filestring)
        print(filestoringpeer)
        keystores.update({0:filessentpeer0})
        print("dictionary")
        print(keystores[0])
    else:
        
        if filestoringpeer=='10.0.130.22':
            filessentpeer1.append(filestring)
            keystores.update({1:filessentpeer1})
            
            print(keystores[1])
        elif filestoringpeer=='10.0.130.23':
            filessentpeer2.append(filestring)
            keystores.update({2:filessentpeer2})
            
            print(keystores[2])
        elif filestoringpeer=='10.0.130.24':
            filessentpeer3.append(filestring)
            keystores.update({3:filessentpeer3})
            
            print(keystores[3])
        else:
            filessentpeer4.append(filestring)
            keystores.update({4:filessentpeer4})
            
            print(keystores[4])
        requeststring= 'STOREOBJ|'+ str(fileposition)+'|'+filestring+'|'+str(peerid)
            
        response=sendRequest(filestoringpeer, hostport, requeststring)
        print(response)        
        endtime=time.time()
        timetaken= endtime-starttime
        print("Total processing time to store files: "+str(timetaken))

def listFiles():
    print("inside list files")
    if len(peer0files)!=0:
        for i in peer0files:
            print("File "+i+" from peer 0 with ip 10.0.130.21" )
    else:
        print()         
    if len(peer1files)!=0:
        for i in peer1files:
            print("File "+i+" from peer 1 with ip 10.0.130.22" )
    else:
        print()
    if len(peer2files)!=0:
        for i in peer2files:
            print("File "+i+" from peer 2 with ip 10.0.130.23" )
    else:
        print()
    if len(peer3files)!=0:
        for i in peer3files:
            print("File "+i+" from peer 3 with ip 10.0.130.24" )
    else:
        print()
    if len(peer4files)!=0:
        for i in peer4files:
            print("File "+i+" from peer 4 with ip 10.0.130.25" )
    else:
        print()
def getFiles(value):
    fileRandomStore()
    if value==0:
        
        print(keystores[0])
        
        
    elif value==1:
        
        print(keystores[1])
        
    elif value==2:
        
        print(keystores[2])
        
    elif value==3:
       
        print(keystores[3])
        
    else:
        
        print(keystores[4])                                                  
def joinChord():
    if hostname == '10.0.130.21':
        node1=node(hostname,hostport,hostname,hostport,hostname,hostport,0)
        node1.printNode()
        node1.listensocket()
        
    else:
        node2=node('',0,hostname,hostport,'',0,1)
        data1='JOIN|'+str(node2.id)+'|'+node2.nn+'|'+str(node2.np)
        responsestring=sendRequest(roothost, hostport, data1)
        data2=responsestring.split('|')
        if (data2[0]=='PREDSUCC') and (data2[2]=='one'):
            node2.pn=data2[1]
            node2.sn=data2[1]
            node2.printNode()
        else:
            node2.pn=data2[1]
            node2.sn=data2[2]
            node2.printNode()
            print("inside many")
            data=updatepredecessor(node2.nn,node2.pn)
            print(data)
            print("update successor")
            updatesuccessor(node2.nn,node2.sn)
            print("inside many")
        node2.listensocket()
        