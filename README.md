# pylo
A simple task execution library written in Python.

## Purpose
Pylo is a very simple task execution framework which provides a couple of extra features not supported by the built-in
Python task execution libraries:

* it snapshots the execution progress, allowing clients to resume execution of tasks (e.g. after a bug has
 been fixed)
* it stores execution errors
* it has a built-in tolerance to execution errors


Pylo was created to deal with uncertain tasks, which fail often, and might have to be iteratively improved. One example 
here could be collecting data of unknown schema, where your goal could be to collect "just enough" data, or to
iteratively improve the collection process by starting somewhere, running it, and capturing the edge-cases by observing
failures. Pylo allows you do to that by snapshotting the execution progress (i.e. what failed and what succeeded), 
tolerating some number of failures, and allowing you to retrieve failures after your execution has finished.


## Example usage

First define a task you would like to execute. For this tutorial, lets assume our task is to download movie details
from IMDB database using https://imdbpy.github.io/. I.e. somewhere in the code we would have:
```
from imdb import IMDb
...
imdb_client = IMDb()
...
```
Also, lets assume we are writing the downloaded movie details to some persistent storage using a class called 
`MovieStore` which has a function `store_movie(movie_id, movie_details)`. E.g. if the underlying persistent storage
is an SQL database, the `MovieStore` class will handle all the details of creating and maintaining a connection, and
SQL needed to insert / update the database. 

Given the above, we can define Pylo task as:
```
def download_movie_details(self, movie_id):
    movie_details = imdb_client.get_movie(movie_id)
    movie_store.store_movie(movie_id, movie_details)
```

Couple of notes about the above:
* `movie_id` is assumed to be some sort of id IMDB associated with each movie. It is basically what we refer 
to as "Pylo task input".
* If the above code fails, it would throw an exception. Pylo doesn't care about the Exception type, it will treat all
exceptions thrown by the above as "task failures".
* There is no output. Pylo does not care about task's output, it assumes you manage the output e.g. by persisting it
in a database.
* Pylo will try not to rerun a successful task twice, but this behaviour is not guaranteed (it can happen if you forcefully close
the Python process which runs Pylo, before Pylo managed to persist its execution state). Therefore, it is recommended
to make tasks idempotent - e.g. the above `movie_store.store_movie(..)` function could check if the movie details has
already been stored for a particular movie, and just return if so. 

With the details above in mind, lets kick off Pylo:
```python
from pylo.execution import Pylo

local_store_dir = "/tmp/pylo/task-cache"
pylo = Pylo.local_multithread(local_store_dir, number_of_workers=2)
```
The above code creates a new Pylo instance, which snapshots the execution state to a local file system 
(to the `local_store_dir`), and creates two threads to execute the task. Now, to kick off the execution:
```
execution_id = pylo.start_from_scratch(movie_ids, download_movie_details)
```
`movie_ids` is a list of IMDB movie ids which you got from somewhere (e.g. by execution some IMDB API call to get it). 
`download_movie_details` is a pointer to the function which we defined before. The above code will block until either:
* the task finishes successfully for all inputs
* the task fails for more inputs than the allowed `max_worker_failures` for each worker (see below, 
here it uses default which is 1000)


To get movie ids for each Pylo succeeded or failed, run:
```
succ_movie_ids, failed_movie_ids = pylo.get_state(execution_id)
```

To get exceptions for failed movie ids, run:
```
exceptions = pylo.get_exceptions(execution_id)
```

To rerun the task only for failed movie ids (e.g. after fixing the underlying problem):
```
new_execution_id = pylo.start_from_past_execution(execution_id, download_movie_details)
```

To change the maximum number of failures tolerated per worker:
```
pylo = Pylo.local_multithread(local_store_dir, 2, max_worker_failures=500)
```

To control how often Pylo snapshots successful / failed inputs to its persistent storage (e.g. file system):
```
pylo = Pylo.local_multithread(local_store_dir, 2, task_executions_before_flush=1)
```
The above will snapshot state after each execution of the task.

To disable exception storage:
```
pylo = Pylo.local_multithread(local_store_dir, 2, store_exceptions=False)
```


## Abstractions
Each `task` is assumed to be of the form:
```text
f(input): void | Exception
```
where `input` is any Python object, and `void | Exception` denotes "either no output or throws an exception when 
the task fails".


Pylo does not care about the output of its task - it is at the liberty of each task to manage the output (e.g. persist 
it in the database). If a task fails, i.e. throws en exception, Pylo would store the exception, and attempt to retry 
the task later. Pylo attempts to always run each successful task, but this is not 
guaranteed. It is therefore recommended that tasks are idempotent w.r.t. their execution and output. 


Tasks in Pylo are grouped into `executions`. Each execution has an `id`, and a `state` which is just a collection of 
inputs of a task. The inputs are divided into:
* `finished` - the task has already been run on these inputs, and it finished without any exceptions
* `unfinished` - either the task has not yet ben run on these inputs, or it failed


Each execution can be resumed from a previous execution (by passing the previous execution id). When resumed,
the execution:
* gets assigned a new execution id
* treats all `finished` inputs of the previous execution as finished, and does not rerun them
* treats all `unfinished` inputs of the previous execution as unfinished, and attempts to rerun them
* does not inherit `exceptions` from the previous execution id


Executions and tasks are run by `workers`. Each worker has an `id`. Also, each worker maintains a set of finished and 
unfinished inputs. No two workers share the same inputs. Furthermore, each worker is configured with the allowed 
number of failures. Note, this number is not "per task", it is "per worker".


## Q/A 
_Where does Pylo snapshot the execution state (i.e. task inputs)?_

Currently it snapshots state only to disk, but it could be easy to extend Pylo to send data to some other 
persistent storage.


_How does Pylo serialize execution state (i.e. task inputs)?_

It uses `pickle`. The nice thing here is that pickle is very flexible with what you can serialize, and so you as a user 
don't have many restrictions on what you can choose as "input" of your task. Please note that using pickle could 
introduce security issues if the data it serializes come from an untrusted source. If this is the case for your Pylo
task inputs, please do not use it. 

Extending Pylo to use other serialization mechanisms (e.g. json) should be very straightforward, feel free to PR.


_How does Pylo run tasks?_

It uses Python threads. In CPython due to GIL, Python threads are not suitable for speeding up CPU-intensive operations.
So if the vast majority of work your tasks do is on the CPU, and you are running this on CPython interpreter, 
you will probably not benefit from running multiple Pylo workers speed-wise. However, if your tasks are IO-bound, 
threads are perfectly fine, and do provide speed boosts. 

Regardless, it should be straightforward to implement other worker models for Pylo, e.g. multi-processing, feel free to 
PR.


_Why was Pylo created?_

Pylo was just a one of side projects I did when learning Python.
