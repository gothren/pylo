# coding=utf-8
from pylo.state import PyloExecutionState, PyloFileSystemExecutionStore


def test_join_two_states():
    state1 = PyloExecutionState(execution_id=1, finished_inputs=[1, 2], unfinished_inputs=[3, 4])
    state2 = PyloExecutionState(execution_id=1, finished_inputs=[5], unfinished_inputs=[])

    joined = state1.join(state2)
    assert joined == PyloExecutionState(execution_id=1, finished_inputs=[1, 2, 5], unfinished_inputs=[3, 4])


def test_split_unfinished_as_many_inputs_as_splits():
    under_test = PyloExecutionState(1, finished_inputs=[1, 2, 3], unfinished_inputs=[4, 5, 6])
    finished, unfinished = under_test.split_unfinished(3)
    assert finished == PyloExecutionState(1, [1, 2, 3], [])
    assert unfinished == [PyloExecutionState(1, [], [4]),
                          PyloExecutionState(1, [], [5]),
                          PyloExecutionState(1, [], [6])]


def test_split_unfinished_more_inputs_then_splits():
    under_test = PyloExecutionState(1, finished_inputs=[], unfinished_inputs=[1, 2, 3, 4, 5, 6, 7, 8])
    finished, unfinished = under_test.split_unfinished(3)
    assert finished == PyloExecutionState(1, [], [])
    assert unfinished == [PyloExecutionState(1, [], [1, 2, 3]),
                          PyloExecutionState(1, [], [4, 5, 6]),
                          PyloExecutionState(1, [], [7, 8])]


def test_split_unfinished_less_inputs_then_splits():
    under_test = PyloExecutionState(1, finished_inputs=[], unfinished_inputs=[1, 2])
    finished, unfinished = under_test.split_unfinished(3)
    assert finished == PyloExecutionState(1, [], [])
    assert unfinished == [PyloExecutionState(1, [], [1]),
                          PyloExecutionState(1, [], [2])]


def test_split_unfinished_inputs_divisible_by_splits():
    under_test = PyloExecutionState(1, finished_inputs=[], unfinished_inputs=[1, 2, 3, 4, 5, 6, 7, 8, 9])
    finished, unfinished = under_test.split_unfinished(3)
    assert finished == PyloExecutionState(1, [], [])
    assert unfinished == [PyloExecutionState(1, [], [1, 2, 3]),
                          PyloExecutionState(1, [], [4, 5, 6]),
                          PyloExecutionState(1, [], [7, 8, 9])]


def test_split_unfinished_inputs_not_divisible_by_splits():
    under_test = PyloExecutionState(1, finished_inputs=[], unfinished_inputs=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    finished, unfinished = under_test.split_unfinished(3)
    assert finished == PyloExecutionState(1, [], [])
    assert unfinished == [PyloExecutionState(1, [], [1, 2, 3, 4]),
                          PyloExecutionState(1, [], [5, 6, 7, 8]),
                          PyloExecutionState(1, [], [9, 10])]


def test_file_system_store_load_worker_state(tmpdir):
    state = PyloExecutionState(execution_id=1, finished_inputs=[1, 2], unfinished_inputs=[3, 4])
    under_test = PyloFileSystemExecutionStore(tmpdir)

    under_test.store_worker_state(execution_id=1, worker_id=1, task_execution_state=state)
    loaded_state = under_test.load_worker_state(execution_id=1, worker_id=1)

    assert state == loaded_state


def test_file_system_store_load_execution_state(tmpdir):
    state1 = PyloExecutionState(execution_id=1, finished_inputs=[1, 2], unfinished_inputs=[3, 4])
    state2 = PyloExecutionState(execution_id=1, finished_inputs=[5], unfinished_inputs=[])

    under_test = PyloFileSystemExecutionStore(tmpdir)

    under_test.store_worker_state(execution_id=1, worker_id=1, task_execution_state=state1)
    under_test.store_worker_state(execution_id=1, worker_id=2, task_execution_state=state2)

    loaded_state = under_test.load_whole_state(execution_id=1)

    assert loaded_state == PyloExecutionState(execution_id=1, finished_inputs=[1, 2, 5], unfinished_inputs=[3, 4])


def test_file_system_store_load_exceptions(tmpdir):
    exception1 = Exception('I run out of memory')
    exception2 = Exception('I run out of cookies')

    under_test = PyloFileSystemExecutionStore(tmpdir)

    under_test.store_task_exception(execution_id=1, exception=exception1)
    under_test.store_task_exception(execution_id=1, exception=exception2)

    loaded_exceptions = under_test.load_task_exceptions(execution_id=1)
    assert [str(e) for e in loaded_exceptions] == [str(exception1), str(exception2)]
