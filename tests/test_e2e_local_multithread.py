from threading import Lock

from pylo.execution import Pylo


def test_single_input(tmpdir):
    calculator = FactorsCalculator()
    numbers_to_factories = [1]

    pylo = Pylo.local(tmpdir, number_of_workers=2)
    execution_id = pylo.start_from_scratch(numbers_to_factories, calculator.compute_factors)

    finished_inputs, unfinished_inputs = pylo.get_state(execution_id)
    assert finished_inputs == [1]
    assert unfinished_inputs == []


def test_multiple_inputs(tmpdir):
    calculator = FactorsCalculator()
    numbers_to_factories = [i for i in range(1, 10000)]

    pylo = Pylo.local(tmpdir, number_of_workers=2)
    execution_id = pylo.start_from_scratch(numbers_to_factories, calculator.compute_factors)

    finished_inputs, unfinished_inputs = pylo.get_state(execution_id)
    assert sorted(finished_inputs) == numbers_to_factories
    assert unfinished_inputs == []


def test_work_not_repeated_when_successful(tmpdir):
    calculator = FactorsCalculator()
    numbers_to_factories = [i for i in range(1, 100)]

    pylo = Pylo.local(tmpdir, number_of_workers=2)
    pylo.start_from_scratch(numbers_to_factories, calculator.compute_factors)

    assert sorted(calculator.inputs_history) == numbers_to_factories


def test_resume_when_failed(tmpdir):
    successful_calc = FactorsCalculator()
    always_fail_cal = FailingFactorsCalculator(num_attempts_to_fail=None)

    numbers_to_factories = [i for i in range(1, 100)]
    fail_for_numbers = [11, 56]

    def calc_fun(n):
        if n in fail_for_numbers:
            return always_fail_cal.compute_factors(n)
        else:
            return successful_calc.compute_factors(n)

    pylo = Pylo.local(tmpdir, number_of_workers=2, max_worker_retries=2)
    execution_id = pylo.start_from_scratch(numbers_to_factories, calc_fun)

    finished_inputs, unfinished_inputs = pylo.get_state(execution_id)
    assert len(finished_inputs) == len(numbers_to_factories) - len(fail_for_numbers)
    assert sorted(unfinished_inputs) == fail_for_numbers

    new_execution_id = pylo.start_from_past_execution(execution_id, successful_calc.compute_factors)
    finished_inputs, unfinished_inputs = pylo.get_state(new_execution_id)
    assert sorted(finished_inputs) == numbers_to_factories
    assert unfinished_inputs == []


# User stories for this:
#   as a user, I would like to define a function, and execute that function on multiple threads
#   as a user, I would like to be able to resume the execution of the function
#   as a user, I would like to clearly see the execution error for my function


class FactorsCalculator:
    def __init__(self):
        self.inputs_history = []

    def compute_factors(self, n):
        self.inputs_history.append(n)
        factors = set()
        for i in range(1, int(n ** 0.5) + 1):
            if n % i == 0:
                factors.add(i)
                factors.add(n // i)
        return factors


class FailingFactorsCalculator:
    def __init__(self, num_attempts_to_fail):
        self._num_attempts_to_fail = num_attempts_to_fail
        self._attempts_so_far = dict()
        self._attempts_lock = Lock()

        self._factors_calculator = FactorsCalculator()

    def compute_factors(self, n):
        with self._attempts_lock:
            if n not in self._attempts_so_far:
                self._attempts_so_far[n] = 0

            self._attempts_so_far[n] += 1

            if self._num_attempts_to_fail is None or \
                    self._attempts_so_far[n] <= self._num_attempts_to_fail:
                raise Exception(f'Failed to compute factors for {n}')

        return self._factors_calculator.compute_factors(n)
