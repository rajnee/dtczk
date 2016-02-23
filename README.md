# dtczk
A Simple Distributed Task Co-ordinator based on Zookeeper

#Basics:

##Zookeeper structure
*DTC root: Root under which all nodes will be created

*Slave Root: Root under which slaves will join processing group by creating ephemeral nodes

*Master Lock: When multiple masters are present, one will be primary holding the lock, others will be standby

*Tasks Root: Under this nodes will be created for each slave server

*Tasks Root/Slave: Tasks for the slave will be assigned from here

##Master Server
An abstract server responsible for assigning tasks to slaves. Implementing classes need to implement schedule().
This will create tasks under slave node

##Slave Server
An abstract server that acts as an agent to process tasks.
This will pick tasks from node "Tasks Root/Slave" and delete the task node when completed
