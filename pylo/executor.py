import logging
import threading

from abc import ABC, abstractmethod
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
            worker_thread = WorkerThread(
                execution_id=execution_state.execution_id,
                worker_id=worker_id,
                task_function=task_function,
                task_state=unfinished_state,
                task_store=task_store,
                max_workers_retry=self._max_workers_retry,
                executions_before_flush=self._executions_before_flush)

            worker_threads.append(worker_thread)

        for worker_thread in worker_threads:
            worker_thread.start()

        for worker_thread in worker_threads:
            worker_thread.join()

        _logger.info('All worker threads finished')


class WorkerThread(threading.Thread):
    def __init__(self, execution_id, worker_id,
                 task_function, task_state, task_store,
                 max_workers_retry, executions_before_flush):
        threading.Thread.__init__(self)
        self.execution_id = execution_id
        self.worker_id = worker_id
        self.task_function = task_function
        self.task_state = task_state
        self.task_store = task_store
        self.max_workers_retry = max_workers_retry
        self.executions_before_flush = executions_before_flush
        self.failures_so_far = 0

    def run(self):
        _logger.info(f'Starting worker {self.worker_id} for execution {self.execution_id}. '
                     f'Executions to perform: {len(self.task_state.unfinished_inputs)}, '
                     f'finished executions: {len(self.task_state.finished_inputs)}')

        while self.task_state.unfinished_inputs:
            if self.failures_so_far >= self.max_workers_retry:
                _logger.error(f'Worker {self.worker_id} failed more than '
                              f'{self.max_workers_retry} times so it will give up')
                self.task_store.store_worker_state(
                    execution_id=self.execution_id,
                    worker_id=self.worker_id,
                    task_execution_state=self.task_state)

                return

            cur_task_input = self.task_state.unfinished_inputs.pop(0)
            try:
                self.task_function(cur_task_input)
            except Exception as e:
                _logger.error(f'Worker {self.worker_id} failed to execute task for input {cur_task_input}. '
                              f'Failures so far: {self.failures_so_far}. Failure message: {str(e)}')

                self.task_state.unfinished_inputs.append(cur_task_input)
                self.failures_so_far += 1
                continue

            self.task_state.finished_inputs.append(cur_task_input)

            if len(self.task_state.finished_inputs) % self.executions_before_flush == 0 or \
                    len(self.task_state.unfinished_inputs) == 0:

                self.task_store.store_worker_state(
                    execution_id=self.execution_id,
                    worker_id=self.worker_id,
                    task_execution_state=self.task_state)
