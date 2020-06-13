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
    numbers_to_factories = [i for i in range(1, 100)]
    fail_for_numbers = [11, 56]

    successful_calc = FactorsCalculator()
    failing_calc = FailingFactorsCalculator(fail_for_numbers)

    pylo = Pylo.local(tmpdir, number_of_workers=2, max_worker_retries=2)
    execution_id = pylo.start_from_scratch(numbers_to_factories, failing_calc.compute_factors)

    finished_inputs, unfinished_inputs = pylo.get_state(execution_id)
    assert len(finished_inputs) == len(numbers_to_factories) - len(fail_for_numbers)
    assert sorted(unfinished_inputs) == fail_for_numbers

    new_execution_id = pylo.start_from_past_execution(execution_id, successful_calc.compute_factors)
    finished_inputs, unfinished_inputs = pylo.get_state(new_execution_id)
    assert sorted(finished_inputs) == numbers_to_factories
    assert unfinished_inputs == []


def test_give_up_when_max_failures_exceeded(tmpdir):
    always_fail_calculator = FailingFactorsCalculator()
    numbers_to_factories = [1]

    pylo = Pylo.local(tmpdir, number_of_workers=2, max_worker_retries=10000)
    execution_id = pylo.start_from_scratch(numbers_to_factories, always_fail_calculator.compute_factors)

    finished_inputs, unfinished_inputs = pylo.get_state(execution_id)
    assert finished_inputs == []
    assert unfinished_inputs == [1]


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
    def __init__(self, only_fail_for_numbers=None):
        self._only_fail_for_numbers = only_fail_for_numbers
        self._factors_calculator = FactorsCalculator()

    def compute_factors(self, n):
        if self._only_fail_for_numbers is None or n in self._only_fail_for_numbers:
            raise Exception(f'Failed to compute factors for {n}')
        else:
            return self._factors_calculator.compute_factors(n)
