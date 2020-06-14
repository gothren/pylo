import logging
import uuid

from pylo.executor import PyloTaskExecutor, PyloLocalMultiThreadExecutor
from pylo.state import PyloExecutionStore, PyloExecutionState, PyloFileSystemExecutionStore

_logger = logging.getLogger(__name__)


class Pylo:
    def __init__(self, task_executor: PyloTaskExecutor, task_store: PyloExecutionStore):
        self._task_executor = task_executor
        self._task_store = task_store

    @classmethod
    def local_multithread(
            cls,
            local_store_dir,
            number_of_workers,
            max_worker_retries=1000,
            task_executions_before_flush=1000,
            store_exceptions=True):

        executor = PyloLocalMultiThreadExecutor(
            number_of_workers=number_of_workers,
            max_workers_retry=max_worker_retries,
            executions_before_flush=task_executions_before_flush,
            store_exceptions=store_exceptions)

        store = PyloFileSystemExecutionStore(local_store_dir)
        return Pylo(executor, store)

    def start_from_past_execution(self, past_execution_id, task_function):
        state_so_far = self._task_store.load_whole_state(past_execution_id)

        # when we start from past execution, instead of overriding it, we create a new execution
        # with state copied from the past execution
        new_execution_id = uuid.uuid4().hex
        new_execution_state = state_so_far.with_execution_id(new_execution_id)
        self._task_executor.execute(new_execution_state, self._task_store, task_function)
        return new_execution_id

    def start_from_scratch(self, task_inputs, task_function):
        _logger.info(f'Starting task execution from scratch with {len(task_inputs)} inputs')
        new_execution_id = uuid.uuid4().hex
        new_execution_state = PyloExecutionState(new_execution_id, [], task_inputs)
        self._task_executor.execute(new_execution_state, self._task_store, task_function)
        return new_execution_id

    def get_state(self, execution_id):
        task_state = self._task_store.load_whole_state(execution_id)
        return task_state.finished_inputs, task_state.unfinished_inputs

    def get_exceptions(self, execution_id):
        return self._task_store.load_task_exceptions(execution_id)
