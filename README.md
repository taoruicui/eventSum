# eventdigest
An event aggregate service 

## Current Implementation
Gator is used in conjunction with Sentry to provide exception reporting and management at Wish. This is currently the main tool we use to ensure the stability of new deployments. In the current system, Gator consists of many different systems: the frontend component, the backend API server, and the exception/log ingestion system. 

## Why does it need to be changed?
Currently Gator and Sentry are regularly overloaded and becomes unresponsive. Sentry is most likely to have problems as a result of a bad deployment so some work needs to be done to improve its performance. This is because Sentry currently records every single exception as a new row in the database. Therefore, there are many rows involved when writing and reading, making the system slow in times of bad deployments and a lot of exceptions. Although Gator is faster, it does not provide the same stack context as Sentry does. Also, the exception aggregation that Gator performs is incorrect, as it only aggregates on Exception name (ex. AttributeError), rather than the full stack trace. 

We want to be able to handle large amounts of data without failure, as well a be horizontally scalable. Andrew Huang recently re-architected Sherlock which is an application debugging tool which uses the same data and has a similar architecture as Gator. So we have some confidence this can be significantly improved. Sherlock is able to handle O(1M) qps easily, gator falls over at O(10K) qps. 

We want a unified system that works for all these types of services. Since Gator and Sherlock are very similar (one tracks exceptions, the other tracks queries), it would be nice to create a generalized service. This way, in the future when something else needs to tracked, a new project doesn’t have to be built from scratch. We also want a system to have a detailed trace of the data like Sentry does, so that we don’t have to cross reference 2 different applications. Not only this, but it would be useful if this system is able to integrate with other systems, specifically opsdb. 

The goal of EventDigest is to create a system where given specific events and their associated data, summarize (aggregate) the events and store them for reference later. This can be contrasted to Eventmaster, where each event is store uniquely (no summarization) as a row in a database. This is necessary for certain applications where extreme granularity is needed (ex: deploys, service restarts, etc). However, in a lot of other cases. If individual event details are not as important, then it is useful to summarize the data. This is because there are performance benefits when querying large groups of events. 


