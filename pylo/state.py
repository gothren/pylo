# coding=utf-8
import math
import os
import pickle
from abc import ABC, abstractmethod
from functools import reduce


class PyloExecutionState:
    def __init__(self, execution_id, finished_inputs, unfinished_inputs):
        self.execution_id = execution_id
        self.finished_inputs = finished_inputs
        self.unfinished_inputs = unfinished_inputs

    def join(self, other_state):
        if other_state.execution_id != self.execution_id:
            raise Exception(f'Unable to join two execution states with different '
                            f'ids: {other_state.execution_id} and {self.execution_id}')

        return PyloExecutionState(
            execution_id=self.execution_id,
            finished_inputs=self.finished_inputs + other_state.finished_inputs,
            unfinished_inputs=self.unfinished_inputs + other_state.unfinished_inputs)

    def with_execution_id(self, new_execution_id):
        return PyloExecutionState(new_execution_id, self.finished_inputs, self.unfinished_inputs)

    def split_unfinished(self, into_number):
        finished_state = PyloExecutionState(self.execution_id, self.finished_inputs, [])
        unfinished_states = \
            [PyloExecutionState(self.execution_id, [], unfinished_chunk)
             for unfinished_chunk in _split_list_into_chunks(self.unfinished_inputs, into_number)]

        return finished_state, unfinished_states

    def __eq__(self, other):
        if isinstance(other, PyloExecutionState):
            return self.execution_id == other.execution_id \
                   and self.finished_inputs == other.finished_inputs \
                   and self.unfinished_inputs == other.unfinished_inputs

        return NotImplemented


class PyloExecutionStore(ABC):
    @abstractmethod
    def load_whole_state(self, execution_id):
        pass

    @abstractmethod
    def load_worker_state(self, execution_id, worker_id):
        pass

    @abstractmethod
    def store_worker_state(self, execution_id, worker_id, task_execution_state):
        pass

    @abstractmethod
    def load_task_exceptions(self, execution_id):
        pass

    @abstractmethod
    def store_task_exception(self, execution_id, exception):
        pass


class PyloFileSystemExecutionStore(PyloExecutionStore):

    def __init__(self, store_directory):
        self.store_directory_path = store_directory

    def load_whole_state(self, execution_id):
        execution_path = os.path.join(self.store_directory_path, str(execution_id))
        worker_ids = [worker_id for worker_id in os.listdir(execution_path) if worker_id != 'meta']
        workers_states = [self.load_worker_state(execution_id, worker_id) for worker_id in worker_ids]

        empty_state = PyloExecutionState(execution_id, [], [])
        return reduce(lambda state1, state2: state1.join(state2), workers_states, empty_state)

    def load_worker_state(self, execution_id, worker_id):
        state_file = os.path.join(self.store_directory_path, str(execution_id), str(worker_id))
        return pickle.load(open(state_file, "rb"))

    def store_worker_state(self, execution_id, worker_id, task_execution_state):
        execution_path = os.path.join(self.store_directory_path, str(execution_id))
        os.makedirs(execution_path, exist_ok=True)

        state_file = os.path.join(execution_path, str(worker_id))
        pickle.dump(task_execution_state, open(state_file, "wb"))

    def load_task_exceptions(self, execution_id):
        exceptions_file_path = os.path.join(self.store_directory_path, str(execution_id), 'meta', 'exceptions')
        if not os.path.exists(exceptions_file_path):
            return []

        exceptions = []
        with open(exceptions_file_path, mode='rb') as ef:
            while True:
                try:
                    exceptions.append(pickle.load(ef))
                except EOFError:
                    break
        return exceptions

    def store_task_exception(self, execution_id, exception):
        meta_files_path = os.path.join(self.store_directory_path, str(execution_id), 'meta')
        os.makedirs(meta_files_path, exist_ok=True)

        exception_file = os.path.join(meta_files_path, 'exceptions')
        pickle.dump(exception, open(exception_file, "ab"))


def _split_list_into_chunks(input_list, number_of_chunks):
    input_list = list(input_list)
    input_list_len = len(input_list)
    chunk_size = math.ceil(input_list_len / number_of_chunks)
    return [input_list[i:i + chunk_size] for i in range(0, input_list_len, chunk_size)]
