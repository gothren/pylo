import logging
from abc import ABC, abstractmethod
from threading import Thread

from pylo.state import PyloExecutionState, PyloExecutionStore

_logger = logging.getLogger(__name__)


class PyloTaskExecutor(ABC):
    @abstractmethod
    def execute(self, execution_state: PyloExecutionState, task_store: PyloExecutionStore, task_function):
        pass


class PyloLocalMultiThreadExecutor(PyloTaskExecutor):
    def __init__(self, number_of_workers, max_workers_retry, executions_before_flush=100):
        self._number_of_workers = number_of_workers
        self._max_workers_retry = max_workers_retry
        self._executions_before_flush = executions_before_flush

    def execute(self, execution_state: PyloExecutionState, task_store: PyloExecutionStore, task_function):
        _logger.info(
            f'Starting execution of tasks. Execution id {execution_state.execution_id}. '
            f'Tasks completed so far {len(execution_state.finished_inputs)}. '
            f'Tasks to complete: {len(execution_state.unfinished_inputs)}.')

        finished_state, unfinished_states = execution_state.split_unfinished(
            into_number=self._number_of_workers)

        # we always write the finished state with the 'worker id' different to all actual workers
        finished_worker_id = 0
        task_store.store_worker_state(
            execution_state.execution_id,
            finished_worker_id,
            finished_state)

        worker_threads = []
        for worker_id, unfinished_state in enumerate(unfinished_states, start=finished_worker_id + 1):
            task_store.store_worker_state(execution_state.execution_id, worker_id, unfinished_state)
            worker_thread = Thread(
                target=_worker_function,
                args=(execution_state.execution_id,
                      worker_id,
                      task_function,
                      unfinished_state,
                      task_store,
                      self._max_workers_retry,
                      self._executions_before_flush))

            worker_threads.append(worker_thread)

        for worker_thread in worker_threads:
            worker_thread.start()

        for worker_thread in worker_threads:
            worker_thread.join()

        _logger.info('All worker threads finished')


def _worker_function(execution_id, worker_id,
                     task_function, task_state, task_store,
                     max_workers_retry, executions_before_flush,
                     failures_so_far=0):

    if failures_so_far >= max_workers_retry:
        _logger.error(f'Worker {worker_id} failed more than {max_workers_retry} times so it will give up')
        task_store.store_worker_state(
            execution_id=execution_id,
            worker_id=worker_id,
            task_execution_state=task_state)

        return

    _logger.info(f'Starting worker {worker_id} for execution {execution_id}. '
                 f'Executions to perform: {len(task_state.unfinished_inputs)}, '
                 f'finished executions: {len(task_state.finished_inputs)}')

    while task_state.unfinished_inputs:
        cur_task_input = task_state.unfinished_inputs.pop(0)
        try:
            task_function(cur_task_input)
        except Exception as e:
            _logger.error(f'Worker {worker_id} failed to execute task for input {cur_task_input}. '
                          f'Failures so far: {failures_so_far}. Failure message: {str(e)}')

            task_state.unfinished_inputs.append(cur_task_input)
            return _worker_function(execution_id=execution_id,
                                    worker_id=worker_id,
                                    task_function=task_function,
                                    task_state=task_state,
                                    task_store=task_store,
                                    max_workers_retry=max_workers_retry,
                                    executions_before_flush=executions_before_flush,
                                    failures_so_far=failures_so_far+1)

        task_state.finished_inputs.append(cur_task_input)
        if len(task_state.finished_inputs) % executions_before_flush == 0 or len(task_state.unfinished_inputs) == 0:
            task_store.store_worker_state(
                execution_id=execution_id,
                worker_id=worker_id,
                task_execution_state=task_state)
