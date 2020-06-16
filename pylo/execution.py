import os
import uuid

from pylo.executor import PyloTaskExecutor, PyloLocalMultiThreadExecutor
from pylo.state import PyloExecutionStore, PyloExecutionState, PyloFileSystemExecutionStore


class Pylo:
    """
    Class which encapsulates all Pylo's functionality, it should be all you need when using Pylo.
    """
    def __init__(self, task_executor: PyloTaskExecutor, task_store: PyloExecutionStore):
        self._task_executor = task_executor
        self._task_store = task_store

    @classmethod
    def local_multithread(
            cls,
            local_store_dir,
            number_of_workers,
            max_worker_failures=1000,
            task_executions_before_flush=1000,
            store_exceptions=True):
        """
        Creates a Pylo instance which uses threads as workers, and persists execution progress to a local directory.
        :param local_store_dir:  the directory where Pylo will persist execution state (successful inputs and failures)
        :param number_of_workers:  the number of threads to create to execute tasks
        :param max_worker_failures:  the maximum allowed task failures per each worker, if a worker exceeds it, it would terminate
        :param task_executions_before_flush:  the number of task executions between snapshotting execution state to disk
        :param store_exceptions:   if set to false, task exceptions will not be persisted
        :return: a new instance of this class
        """

        executor = PyloLocalMultiThreadExecutor(
            number_of_workers=number_of_workers,
            max_worker_failures=max_worker_failures,
            executions_before_flush=task_executions_before_flush,
            store_exceptions=store_exceptions)

        pylo_store_dir = os.path.join(local_store_dir, 'pylo')
        store = PyloFileSystemExecutionStore(pylo_store_dir)
        return Pylo(executor, store)

    def start_from_past_execution(self, past_execution_id, task_function):
        """
        Starts a new execution which inherits state from the previous execution, i.e. "resumes" the previous execution.

        The new execution would inherit finished and unfinished inputs. It would not rerun the task over the finished
        (successful) inputs. It would rerun the task over all unfinished inputs. The new execution will not inherit
        exceptions thrown by the previous execution.

        The input task function should throw an exception when the task fails. Pylo will ignore its output, and if
        there is no exception, it would assume that the task finished successfully.

        :param past_execution_id:  the id of the previous execution which we would like to resume
        :param task_function:   the task we would like to accomplish, can be anything
        :return:  the execution id of the new, resumed execution of the input task
        """

        state_so_far = self._task_store.load_whole_state(past_execution_id)

        # when we start from past execution, instead of overriding it, we create a new execution
        # with state copied from the past execution
        new_execution_id = uuid.uuid4().hex
        new_execution_state = state_so_far.with_execution_id(new_execution_id)
        self._task_executor.execute(new_execution_state, self._task_store, task_function)
        return new_execution_id

    def start_from_scratch(self, task_inputs, task_function):
        """
        Starts a new execution of the input task.

        The input task function should throw an exception when the task fails. Pylo will ignore its output, and if
        there is no exception, it would assume that the task finished successfully.

        :param task_inputs:  the inputs of the task, should be iterable (e.g. list)
        :param task_function:   the task we would like to accomplish, can be anything
        :return:
        """
        new_execution_id = uuid.uuid4().hex
        new_execution_state = PyloExecutionState(new_execution_id, [], task_inputs)
        self._task_executor.execute(new_execution_state, self._task_store, task_function)
        return new_execution_id

    def get_state(self, execution_id):
        """
        Returns the state of a given execution.

        The output state is a tuple which consists of a list of task inputs over which the task run successfully,
        and a list of unfinished task inputs. The unfinished inputs either failed, or have not run yet (e.g. because
        Pylo program was terminated, or because the max_worker_failures was exceeded for all workers.

        :param execution_id:  the id of the execution which state we would like to query
        :return:   a tuple which consists of finished and unfinished task inputs
        """
        task_state = self._task_store.load_whole_state(execution_id)
        return task_state.finished_inputs, task_state.unfinished_inputs

    def get_exceptions(self, execution_id):
        """
        Returns exceptions thrown when running the execution with the given id.
        :param execution_id:  the id of the execution which state we would like to query
        :return:  a list which consists of Exception
        """
        return self._task_store.load_task_exceptions(execution_id)
