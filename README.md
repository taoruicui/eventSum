# exceptionmaster
An exception aggregate service 

## Current Implementation
Gator is used in conjunction with Sentry to provide exception reporting and management at Wish. This is currently the main tool we use to ensure the stability of new deployments. In the current system, Gator consists of many different systems: the frontend component, the backend API server, and the exception/log ingestion system. 

## Why does it need to be changed?
Currently this system is regularly overloaded and becomes unresponsive. Gator is most likely to have problems as a result of a bad deployment so some work needs to be done to improve its performance. This is because Gator currently records every single exception as a new row in the database. Therefore, there are many rows involved when writing and reading, making the system slow in times of bad deployments and multiple exceptions. 

We want Gator to be able to handle large amounts of data without failure, as well a be horizontally scalable. Andrew Huang recently re-architected Sherlock which is an application debugging tool which uses the same data and has a similar architecture. So we have some confidence this can be significantly improved. Sherlock is able to handle O(1M) qps easily, gator falls over at O(10K) qps. Not only this, but it would be useful if Gator is able to integrate with other systems, specifically opsdb. This would mean that gator should belong as its own service, outside of clroot. 

