#CREATED BY EGE ATA CEYLAN

import random
import os
import threading

import zmq
from multiprocessing import Process
import time

import sys

displayLock = threading.Lock()


def leader(nodeId: int ,starter: bool,numProc):
    pid = os.getpid()
    print("PROCESS STARTS: ",pid," ",nodeId," ",str(starter))
    global leaderFlag
    global terminateFlag
    leaderFlag = 0
    terminateFlag = 0
    listener = threading.Thread(target=responder, args=(nodeId,numProc))
    listener.start()

    time.sleep(4)
    context = zmq.Context()
    messageSender = context.socket(zmq.PUB)
    currentAddress= "tcp://127.0.0.1:" + str(5550+nodeId)
    #print("This is the current address: ",currentAddress) for debug purpose
    messageSender.bind(currentAddress)

    context = zmq.Context()
    responseReciever = context.socket(zmq.SUB)

    for process in range(0, numProc):  # 0 to numprocess
        currentAddress = "tcp://127.0.0.1:" + str(6550 + process)
        responseReciever.connect(currentAddress)
    responseReciever.subscribe("RESPONSE")
    poller = zmq.Poller()
    poller.register(responseReciever, zmq.POLLIN)


    time.sleep(3)

    currentMessage = "LEADER "+str(nodeId)

    if(starter):
        messageSender.send_string(currentMessage)
        displayLock.acquire()
        print("PROCESS MULTICASTS LEADER MSG: ",nodeId)
        displayLock.release()

        recievedFromBigger = False
        should_continue = True
        time.sleep(7)
        while should_continue:
            socks = dict(poller.poll(0))

            if responseReciever in socks and socks[responseReciever] == zmq.POLLIN:
                message = responseReciever.recv_string()
                topic, senderNodeId ,recieverNodeID = message.split()

                if (int(recieverNodeID)== nodeId and int(senderNodeId) > nodeId):
                    recievedFromBigger = True

            else:
                should_continue=False
        if (not recievedFromBigger):
            #print("NO MESSAGE RECIEVED") debug purpose
            currentMessage = "TERMINATE " + str(nodeId)
            messageSender.send_string(currentMessage)
            print("PROCESS BROADCASTS TERMINATE MSG: " + str(nodeId))

    else:

        while(leaderFlag == 0 and terminateFlag == 0):
            #print("waiting") debug purpose
            wait = 1
        time.sleep(15)
        if(leaderFlag ==1 and terminateFlag==0):
            #print("Node ",str(nodeId),"Will start to send leader messages") debug purpose
            messageSender.send_string(currentMessage)
            displayLock.acquire()
            print("PROCESS MULTICASTS LEADER MSG: ", nodeId)
            displayLock.release()

            should_continue = True
            recievedFromBigger = False
            time.sleep(7)
            while should_continue:
                socks = dict(poller.poll(0))

                if responseReciever in socks and socks[responseReciever] == zmq.POLLIN:
                    message = responseReciever.recv_string()
                    topic, senderNodeId, recieverNodeID = message.split()

                    if(int(recieverNodeID) == nodeId and int(senderNodeId) > nodeId):
                        recievedFromBigger = True

                else:
                    should_continue=False
            if(not recievedFromBigger):
                #print("NO MESSAGE RECIEVED") debug purpose
                currentMessage = "TERMINATE "+ str(nodeId)
                messageSender.send_string(currentMessage)
                print("PROCESS BROADCASTS TERMINATE MSG: "+str(nodeId))
        else:
            justTerminate = 1



    listener.join()
    return 1



def responder(nodeId: int, numProc):
    print("RESPONDER STARTS: ",nodeId)

    context = zmq.Context()
    messageReciever = context.socket(zmq.SUB)

    # CONNECT TO ALL PORTS OF PROCESSES
    global terminateFlag
    global leaderFlag
    terminateFlag = 0
    leaderFlag = 0

    for process in range(0,numProc):# 0 to numprocess
        currentAddress = "tcp://127.0.0.1:" + str(5550 + process)
        messageReciever.connect(currentAddress)

    messageReciever.subscribe("LEADER")
    messageReciever.subscribe("TERMINATE")


    time.sleep(5)
    context = zmq.Context()
    responseSender = context.socket(zmq.PUB)
    xx = "tcp://127.0.0.1:" + str(6550 + int(nodeId))

    responseSender.bind(xx)

    terminate = False
    while not terminate:
        message = messageReciever.recv_string()

        topic, senderNodeId = message.split()

        if(topic == "LEADER" and int(senderNodeId) < nodeId):
            leaderFlag = 1


            responseSender.send_string("RESPONSE " +str(nodeId) +" "+str(senderNodeId))
            displayLock.acquire()
            print("RESPONSER RESPONDS ",nodeId," ",senderNodeId)
            displayLock.release()
            
        elif(topic=="TERMINATE"):
            #print("TERMINATE MESSAGE RECIEVED") debug purpose
            terminateFlag =1
            terminate = True

    return 1




if __name__ == '__main__':
    numProc = int(sys.argv[1])
    numAlive  = int(sys.argv[2])
    numStarters = int(sys.argv[3])



    alives = random.sample(range(0,numProc),numAlive)

    starters = random.sample(alives,numStarters)

    allProcesses = []

    print("Alives:")
    print(alives)
    print("Starters:")
    print(starters)

    for process in alives:
        if(process in starters):
            p = Process(target=leader, args=(process,True,numProc))
            p.start()
            allProcesses.append(p)
        else:
            p = Process(target=leader, args=(process,False,numProc))
            p.start()
            allProcesses.append(p)



    #Waiting for all threads to join
    for process in allProcesses:
        process.join()
