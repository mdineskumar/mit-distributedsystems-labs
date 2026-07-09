The mental model

Think of it as one coordinator with a pile of tasks, and N workers that are dumb loops:

Input files (pg-0.txt, pg-1.txt, ...)
        │
        ▼
   [Map phase]  nMap tasks, one per input file
        │  each writes nReduce intermediate files: mr-<mapTaskId>-<reduceTaskId>
        ▼
   [Reduce phase]  nReduce tasks, one per reduce-bucket
        │  each reads mr-*-<reduceTaskId> from every map task
        ▼
   mr-out-<reduceTaskId>

Two invariants drive everything:
- Reduce can't start until all maps are done (a reduce task needs the intermediate file from every map task, not just some).
- A worker can die mid-task, so the coordinator must be able to hand the same task to someone else without corrupting output.

Coordinator: a state machine, not a to-do list

Your Coordinator struct is basically right. The state machine is:

Phase = Map
  → AskTask hands out Idle map tasks
  → when ALL map tasks are Completed → Phase = Reduce
Phase = Reduce
  → AskTask hands out Idle reduce tasks
  → when ALL reduce tasks are Completed → Phase = Done

Three RPCs, not one:
1. AskTask — worker asks "what do I do?" (you have this)
2. ReportTask (you don't have this yet) — worker tells coordinator "I finished task X" so the coordinator can mark it Completed and check phase transitions. Without this, tasks assigned InProgress never become Completed, and you can never move Map → Reduce.
3. (Implicitly) the "please exit" pseudo-task via TaskType.Exit in the reply, once everything is done.

Where your coordinator.go currently falls short

- AskTask never takes the lock (c.mu). Multiple workers calling concurrently will race on c.mapTasks[i].status. Grab c.mu.Lock()/defer Unlock() at the top.
- Nothing ever moves c.phase from MapTask to ReduceTask. You need a helper like allMapTasksDone() that AskTask (or a separate check) consults.
- Nothing marks a task Completed — there's no RPC handler for the worker to call back and say "done." Right now a task goes Idle → InProgress and just... stays there forever.
- No timeout/reissue logic. You store startTime but nothing reads it. You need something that, when a task is InProgress for >10s, flips it back to Idle so it gets handed out again — either a background goroutine that sweeps periodically, or a check inline in AskTask before deciding a task is unavailable.
- Done() is a stub — should return true once phase is past Reduce and all reduce tasks are Completed.
- In the reduce branch, reply.TaskType = ExitTask as soon as there are no Idle reduce tasks — but tasks that are InProgress aren't done yet. A worker could get ExitTask while reduce work is still running elsewhere. You need to distinguish "no idle task right now, wait" from "everything is actually Completed, exit."

Where your worker.go currently falls short

- Worker() calls AskTask() once and returns. It needs to be a loop: ask → get a task type → act on it → ask again, forever until told to exit.
- It never calls mapf/reducef, never reads input files, never writes intermediate/output files. That's the actual work — steal the file-reading/writing/sorting logic from mrsequential.go you already read.
- It never handles WaitTask (sleep briefly, ask again) or ExitTask (return from Worker()).
- It never reports completion back to the coordinator.

A concrete build order (so you're never stuck on "what next")

1. Add c.mu.Lock()/Unlock() to AskTask.
2. Add a ReportTask(args, reply) RPC: worker sends back {TaskType, TaskId} after finishing; coordinator marks that task Completed.
3. Add allDone(tasks []Task) bool helper; use it in AskTask to flip c.phase from Map→Reduce, and in Done().
4. In AskTask, before "no idle task" → WaitTask, first sweep for InProgress tasks older than 10s and reset them to Idle.
5. In worker: turn Worker() into a loop; for MapTask, read the file, call mapf, partition results into nReduce buckets by ihash(key) % nReduce, write each bucket to mr-<mapId>-<reduceId> (use JSON encoding as the PDF hints), then call ReportTask.
6. For ReduceTask: read all mr-*-<reduceId> files, decode JSON, sort by key, call reducef per key, write mr-out-<reduceId>, call ReportTask.
7. For WaitTask: time.Sleep briefly, loop again. For ExitTask: return.
8. Only then worry about atomic rename (temp file + os.Rename) and the crash test.