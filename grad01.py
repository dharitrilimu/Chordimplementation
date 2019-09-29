'''
Created on Apr. 15, 2019

@author: dharitri
'''
import socket
import random
import string
import time
listof_ids=[]
data={0:'10.0.130.21',1:'10.0.130.22',2:'10.0.130.23',3:'10.0.130.24',4:'10.0.130.25'}

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
                if self.sn==hostname:
                    print("inside JOIN if block")
                    self.pn=nodevalues[2]
                    self.pp=int(nodevalues[3])
                    self.sn=nodevalues[2]
                    self.sp=int(nodevalues[3])
                    self.printNode()
                    sentdata='PREDSUCC|'+self.nn+'|'+'one'
                    listof_ids.append(int(nodevalues[1]))
                    for i in listof_ids:
                        print("existing nodes having ids are : ")
                        print(i)
                    listof_ids.sort()
                    c.sendall(sentdata.encode())
                else:
                    print("inside JOIN else block")
                    listof_ids.append(int(nodevalues[1]))
                    for i in listof_ids:
                        print("existing nodes having ids are : ")
                        print(i)
                    listof_ids.sort()
                    
                    
                    if listof_ids.index(int(nodevalues[1]))== 4:
                        print('inside if block')
                        succ=data[listof_ids[0]]
                        pred=data[listof_ids[listof_ids.index(int(nodevalues[1])-1)]]
                        response='PREDSUCC|'+pred+'|'+succ+'|'+'many'
                        c.sendall(response.encode())
                    else:
                        print("inside else block root join")
                        index=listof_ids.index(int(nodevalues[1]))
                        print(index)
                        if index==(len(listof_ids)-1):
                            succkey=0
                        else: 
                            succkey=listof_ids[index+1]
                        print(succkey)
                        succ=data[succkey]
                        print(succ)
                        predkey=listof_ids[index-1]
                        print(predkey)
                        pred=data[predkey]
                        print(pred)
                        response='PREDSUCC|'+pred+'|'+succ+'|'+'many'
                        print(response)
                        c.sendall(response.encode())
                   
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
            elif nodevalues[0]=='UPDATECHORD':
                listof_ids.remove(int(nodevalues[1]))
                for i in listof_ids:
                    print("existing nodes having ids are : ")
                    print(i)
                c.sendall(("Node with id : "+nodevalues[1]+"has been removed from chord").encode())
            elif nodevalues[0]=='STOREOBJ':
                position=int(nodevalues[1])
                frompeerid=int(int(nodevalues[3]))
                if position in listof_ids:
                    #store in rootnode
                    if position==0:
                        if frompeerid==0:
                            peer0files.append(nodevalues[2])
                            filestore.update({0:peer0files})
                            print("file stored in grad03!!!Request generator grad01")
                        elif frompeerid==1:
                            peer1files.append(nodevalues[2])
                            filestore.update({1:peer1files})
                            print("file stored in grad03!!!Request generator grad02")
                        elif frompeerid==2:
                            peer2files.append(nodevalues[2])
                            filestore.update({2:peer2files})
                            print("file stored in grad03!!!Request generator grad03")
                        elif frompeerid==3:
                            peer3files.append(nodevalues[2])
                            filestore.update({3:peer3files})
                            print("file stored in grad03!!!Request generator grad04")
                        else:
                            peer4files.append(nodevalues[2])
                            filestore.update({4:peer4files})
                            print("file stored in grad03!!!Request generator grad05")               
                        print("file is stored successfully in root")
                        c.sendall("STOREDINROOT".encode())
                    #store in othernode
                    else:
                        print("in else for file store")
                        index=listof_ids.index(position)
                        filestoringpeer=data[index]
                        print(filestoringpeer)
                        c.sendall(filestoringpeer.encode())
                
                #store in successor           
                else:
                    print("file stored in its successor")                  
                    
                    if position>listof_ids[len(listof_ids)-1]:
                        if frompeerid==0:
                            peer0files.append(nodevalues[2])
                            filestore.update({0:peer0files})
                        elif frompeerid==1:
                            peer1files.append(nodevalues[2])
                            filestore.update({1:peer1files})
                        elif frompeerid==2:
                            peer2files.append(nodevalues[2])
                            filestore.update({2:peer2files})
                        elif frompeerid==3:
                            peer3files.append(nodevalues[2])
                            filestore.update({3:peer3files})
                        else:
                            peer4files.append(nodevalues[2])
                            filestore.update({4:peer4files})
                        print("printing tuple")
                        print(filestore[2])
                        print("end of tuple")
                        print("file is stored successfully in root")    
                        c.sendall("STOREDINROOT".encode())
                    else:
                        j2 = [i for i in listof_ids if i > position]
                        filestoringpeer=data[j2[0]]
                        print(filestoringpeer)
                        c.sendall(filestoringpeer.encode())
                            
                      
                              
            else:
                print("no request") 
hostname='10.0.130.21'
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
def successor():
    requeststring='FINDSUCCESSOR'
    successor=sendRequest(hostname, hostport, requeststring)
    print(successor)
def leaveChord():
    print("Sorry!It can not leave Chord!!!You are in master node.")
   
def fileRandomStore():
    starttime=time.time()
    peerid=0
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
    
    filestoringpeer=sendRequest("10.0.130.21", hostport, requeststring1)
    
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
                                   
def joinChord():
    if hostname == '10.0.130.21':
        node1=node(hostname,hostport,hostname,hostport,hostname,hostport,0)
        listof_ids.append(int(node1.id))
        node1.printNode()
        node1.listensocket()
        
    else:
        node1=node(hostname,hostport,hostname,hostport,hostname,hostport,0)