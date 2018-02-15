Two roles:
 * Coordinator: Client endpoint, and cluster coordinator
 * Worker: Executor

Common Services:
* Node Service
* Manager
* Node Monitor
  * Uptime
  * NodeState (Normal or Abnormal)
  * NodeStatus
  * Max/Available Memory and Memory Pool Statistics
  * Process/System CPU Load
* EventListener
  * Deliver events to a destination service

Services in Coordinator:
* Client API Service
* Node Manager
* Query Manager
  * APIs exported:
    * Blocked/Queued/Running/Killed/Failed queries
    * Query Stats
* Node Scheduler
* Cluster Monitor
  * Cluster Stats

Services in Worker
 * Executor
 * Memory Storage
 * Worker Scheduler
 