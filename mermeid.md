#
# Mermaid Diagram for NAQ Architecture
This diagram illustrates the interaction between the client, NAQ processes, and NATS server components
 It shows how jobs are enqueued, processed, and results stored.
 
```mermaid
graph TD
    subgraph "Your Application"
        Client[Client]
        PythonCode[Python Code]
    end

    subgraph "NAQ Processes"
        Worker[Worker]
        Scheduler[Scheduler]
    end

    subgraph "NATS Server (JetStream)"
        QueueStream[Queue Stream]
        ResultStore[Result KV Store]
        ScheduledJobs[Scheduled Jobs KV]
    end

    Client -- "1. Enqueue Job" --> QueueStream
    Scheduler -- "7. Check for Due Jobs" --> ScheduledJobs
    ScheduledJobs -- "Job is Due" --> Scheduler
    Scheduler -- "8. Enqueue Due Job" --> QueueStream
    Worker -- "3. Fetch Job" --> QueueStream
    Worker -- "4. Execute Function" --> PythonCode
    PythonCode -- "5. Return Result" --> Worker
    Worker -- "6. Store Result" --> ResultStore
```