# Multi-processes using request/cache controls

Mini 2 takes us from internal process parallelization to multi-process 
coordination and distributed queries.. 

Per class request, the data set you used in mini 1 can be use in mini 
2, or replaced as you see fit though we want to move a way from linear
searching and adopt different techniques and organizaiton.

We are going to extend the leader-adv lab to build a backbone for a
multi-process network for processes to gather and scatter requests
and data. The choices of the technology stack is pretaining to back-
end development, not front-end so, use of web-services while easily
integrated to, do not represent performance and data structures.

Key concepts and technologies/tools you will be working with:

   1. Distributed data and queries
   2. Temporal and spatial decoupling
   3. Communication/Messaging with gRPC. While Thrift and other choices 
      exist, the engineering is the same.
   4. Python. That's three, but you know Python so it doesn't count.
   5. Streaming. Parallel and dynamic chunk sizing. Tuning your system
      of processes to optimize on the fly payload sizes. 

## Mini Goals

The teams are building a request mechanism to delay (optimize) data
transfer. Think of it as a on demand, distributed result set, or a multi-
chunk engine response, or a CDN x Kafka x Hadoop scatter-gather system. 

Question: What is the most performant (resources, time) way, how to conserve 
memory, and to allow fairness between end-points (requests). 

Side note: Can requests be anticapated?

There are two aspects of the mini that are critical to your team's success. 
The first (flexible assignments using overlays) much of the work resides 
here because we are setting up the features to look at optimizing request-
response pressure. 

Second, fairness and balance.


### Basecamp

So, the first challenge is to setup a working code base to build from. That's
easy and hard. First, look at the gRPC loop lab for inspiration. A big note here, 
gRPC provides async and streaming APIs, but you can not use them. There is good 
reason for this. First, how do you develop your algorithm if a large portion of 
the stream-control is hidden and out of your hands? Second, it will detract from 
the possible points for the mini.

Basecamp is a collection of processes (A,B,C,D,E,F,G,H,I) communicating using gRPC 
across a minimum of two computers (1,2). 

Overlay choices. There are two designs for you to choose from:

   1. Tree: In an overlay configuration of AB, BC, BD, BE, EF, ED, EG, AH, AG, AI. 

      * Team Blue: A,B,D,H
      * Team Yellow: C,E,F,G,I

      Notes: 1) You don't have to name your teams blue and yellow. This is just for 
      discussion, 2) Teams can increase the depth by adding more servers, but they
      cannot flatten the tree or simplify the edges.
      
   2. A 3x3 grid: Where edges are horizontal and vertical. E.g, AB, AD. Where AE 
      does not exist.

      A  B  C
      D  E  F
      G  H  I

Host limits: The two accepted configurations are as follows.

   * A two computer minimum configuration 
   * A three computer configuration 

While developming, individual work, it is okay to have all servers on the same 
host. However, for running you will need 2 or 3.

Preparation of your basecamp is critical. So, let's briefly go
over what it should and should not be.


#### Features and Constraints

   * DO NOT hardcode a server's identity (e.g., A), role, or hostname
   * You will need a server and a client written in C++, and also a server 
     in Python.
   * All servers are asynchronous and trusted.
   * Final run with at least two computers. Three is okay as well.
   * Organize your code into logical sub-directories
   * Run each server in a separate shell (e.g., bash), test from a shell.
   * Do not test from VMs in vscode or similar IDE VM crap.
   * Think about how you are going to test your system.
   * Spirit of the mini is to explore distributed processes. There is a fine
     edge between innovation and gaming the system (mini). What is the
     short-term verses long-term advantage to you?


#### Avoidance hints

   * Do not use the IDE to run your code.
   * Remove or isolate dead or incomplete code from your main work
   * This is more work than mini 1 so, don't wait until the last week 
     to get started.
   * Details matter so, don't focus on making an application.  
   * Code organization is important

*Hint. gRPC source code has many examples to help design/code.*


### Discovery

It is not intended for this mini to have processes discover each other
dynamically. Rather a mapping can exist that provides guidance to each
process' edges (connections). Again, do not hard code settings within 
the code.


## Technical Constraints

Let us recap the guardrails mentioned above:

   * Python, C/C++, NOT javascript (node), java, C#, or other stuff.
   * Minimize third party libraries. Please do not use Anaconda stuff, 
     we are focusing at the service's communication transport layers.
   * No UX/UI stuff either. This includes test-based CLI menu systems.
   * No IDE VMs
   * Do not put responses in shared memory


### Using Basecamp

Once your basecamp has been established (how did you verify this?), we
can proceed onto the second part, caching and request (messaging). 
Let's ease the coding distractions; use the data (fire) and reuse whatever
code you want from mini 1. 

First data requests can be received only by A. Consider A as a
portal (public facing) into a private network if that helps. 

Of the communication styles, we are interested in one-way (request),
two-way (request-response), and one-to-many (broadcast) though a
one-to-many is often viewed as a multi-part point-to-point, and less
of a literal UDP. Why?

Challenges - How do you create a two-way without using a 
request-response (two-way) style? For example, a client sends a 
query to A. A then attempts to answer the question by contacting its
peers (team leaders). The results are gathered and the client is
returned the results,but the result set is too big. Also, what if the
client cancels the request before it is completed. Or the client
looses its connection to A? Or abandons a requests? Are there other
considerations? 

It is expected the peers are independently contributing to the response 
(data). Each team has a subset of the data space (no sharing, no replication, 
no mirroring). 

Lastly, only A is allowed to reply to the client through gRPC (the 
results). 


## Using what we understand (mini 1)

Code experiences from mini-1 like for-loop parallelization scheduling, 
overloading, and testing practices can be insightful. 

To consider:

   * What do you consider are key performance/adoption indicators? 
       * The criteria to measure? Performance, time and effort, LoC, 
         cross-language, cross-OS, community support and tools, adoption 
         trajectories, hype curve, and so many others to choose from.  
       * Hint: Use more than one.
       * How do you measure time (dt)?
   * Realistic expectations and modeling - choose carefully. 
       * Professional organization and use of accepted style.
       * Using realistic data structures. I.e. strings for everything is not 
         a good choice. 
       * Coding a fixed response, chat-like, or a spoke-hub is a poor choice.


## Starting points

A couple code examples (leader-adv, shared memory, loop-grpc) have been uploaded to
the labs section; they provide inspiration seeds for your team to brainstorm what 
you want to consider. Other sources (repos - source code like gRPC from gitHub) are
at your disposal. Do not limit yourselves to what is talked about in Class/Canvas.

VERY IMPORTANT (that's why it is in all caps) - Use realistic data structures. I would 
ask that you are mindful in constructing classes/code and test cases that are 
representative to how the code would be used in an application setting. This includes
integers when integers are needed, floats/doubles, bools, etc. This will affect scoring.
It is not realistic to believe a developer/integrator of your code would just use the 
raw string. Rather a type-correct data struct (e.g., int, double, bool, string) is 
required.


## Overlays

As mentioned above the network to construct is defined as a tree of processes. We are
not interested in a flat (one-to-many, aka two-tier) design. Don't do it.

This will create a couple challenges to you that a flat ignores. First, how does one
configure servers to know their share edges. Second, how do you forward
requests, and lastly, how do you provide context (match) requests to replies.

Edges are asynchronous but, do not have to be bi-directional? It is up to
your team to determine how to approach this within the limits set forth above.


## Balance vs replication

You have a choice: is your design focusing on balance, replication? 

Balance from what perspective? Exactly. 


# That's a lot

To recap, build a collection (2) of processes that work to solve a problem where we
can control request-response fairness through a segmented-reply, like multi-part or
result sets. We want the calling layer to be able to retrieve the data in reasonable 
chunks for consumption and to manage memory and bandwidth. There is a lot of words 
dedicated to what you should do and not do, it is there to prevent finding loop 
holes within the description. The goal is not to exploit loop holes, it is to build 
skills. 

Do we can consider failures, it was not mentioned, but if your team wants to explore
how failure-recovery work, e.g,, a server (process) dies, or other cases, please
do, but remember there is a limited amount of time.


# Except for this

Do not hard code settings within your code.
