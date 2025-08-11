# Bully Leader Election Algorithm
## Overview
This project implements the Bully Leader Election (BLE) algorithm. The BLE algorithm allows multiple nodes in a distributed system to elect a leader when the current leader becomes unavailable.
The implementation uses Python 3 with ZeroMQ (ZMQ) sockets and the Publish–Subscribe mechanism for reliable multicasting.

## Algorithm Summary
Multiple nodes can initiate the leader election process simultaneously.

Nodes send LEADER messages to all nodes with higher IDs.

If a higher-ID node responds, the sender becomes a passive listener.

The highest-ID node announces itself as leader by broadcasting a TERMINATE message.

All nodes update their state upon receiving a TERMINATE message.

## Features
Multi-process simulation of distributed nodes.

Separate listener thread per node for concurrent message handling.

Reliable communication using ZMQ Pub–Sub sockets.

Randomized selection of alive nodes and starter nodes.

Console logging of all process starts, responder starts, messages sent, and message responses.

## Usage
Run the program from the command line:

    python ble_algorithm.py numProc numAlive numStarters

Arguments:

numProc: Total number of nodes.

numAlive: Number of nodes that are alive (online).

numStarters: Number of nodes that initiate the protocol.

Constraints:

0 < numStarters ≤ numAlive ≤ numProc

### Example
     python ble_algorithm.py 10 4 2

#### Example Output
Alives: [8, 9, 0, 3]  
Starters: [9, 3]  
PROCESS STARTS: 42004 8 False  
RESPONDER STARTS: 8  
PROCESS STARTS: 49092 9 True  
RESPONDER STARTS: 9  
PROCESS STARTS: 42836 3 True  
RESPONDER STARTS: 3  
PROCESS STARTS: 40496 0 False  
RESPONDER STARTS: 0  
PROCESS MULTICASTS LEADER MSG: 9  
PROCESS MULTICASTS LEADER MSG: 3  
RESPONDER RESPONDS 9 3  
RESPONDER RESPONDS 8 3  
PROCESS MULTICASTS LEADER MSG: 8  
RESPONDER RESPONDS 9 8  
PROCESS BROADCASTS TERMINATE MSG: 9  

# Implementation Details
## Leader Method
Runs in the main thread of each process.

Responsibilities:

1.Start the responder thread.

2.Wait for election trigger: either because the process is a starter or receives a LEADER message via the responder.

3.Multicast a LEADER message to all nodes with higher IDs.

4.Wait for responses:

    -If a response from a higher-ID node is received, become passive.

    -If no response is received within a timeout, broadcast a TERMINATE message announcing itself as the leader.

5.Terminate gracefully after receiving a TERMINATE message.

## Responder Method
Runs in a separate thread for each process.

Responsibilities:

1.Subscribe to all alive nodes’ ports.

2.Listen for incoming messages (LEADER, TERMINATE).

3.On receiving:

    TERMINATE: Notify the leader thread and terminate.

    LEADER:

        If sender’s ID is lower, send a RESP message back.

        If not already participated, initiate its own leader election by notifying the leader thread.

4.Handle concurrency between leader and responder using shared variables and locks.



# Communication Details
Ports: Each process binds to localhost at port 5550 + nodeID.

Pub–Sub: Each process publishes to its own port and subscribes to all others.

Timeouts: zmq.Poller or socket timeouts prevent blocking on message receive.

Message loss prevention: Optional small delays before sending to allow subscriber connections.




## Requirements/Dependincies
Python 3.x

ZeroMQ (pyzmq) library

Install dependencies:

    pip install pyzmq
