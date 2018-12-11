###
###
#
# Test runner that reads test case configuration, turns it into pytest test cases and executes them
#
###
###


import os
import json
import great_expectations as ge
from .. super_pipeline import SuperPipeline


def pytest_generate_tests(metafunc):
    """
    This method (and the method below) use pytest parametrization (https://docs.pytest.org/en/latest/parametrize.html#pytest-generate-tests)
    to create test cases out of every test case configuration in the test configuration file.

    """

    dir_path = os.path.dirname(os.path.realpath(__file__))
    # This is the file where you define all test cases
    config_file_path = os.path.join(dir_path, "great_expectations_tests.json")

    parametrized_tests = []
    ids = []
    file = open(config_file_path)
    test_configuration = json.load(file)

    for test_case_config in test_configuration['test_cases']:
        parametrized_tests.append(test_case_config)
        ids.append("Great Expectations: "+test_case_config["title"])

    metafunc.parametrize(
        "test_case",
        parametrized_tests,
        ids=ids
    )

def test_case_runner(test_case):
    """
    This method gets test case structure and calls the method that executes this test case.
    You should adjust this methdd to reflect your test case configuration attributes
    """

    if 'title' not in test_case:
        raise ValueError("Invalid test configuration detected: 'title' is required.")

    if 'input_file_path' not in test_case:
        raise ValueError("Invalid test configuration detected: 'input_file_path' is required.")

    if 'expectations_config_path' not in test_case:
        raise ValueError("Invalid test configuration detected: 'expectations_config_path' is required.")

    execute_great_expectations_test_case(
        test_case["title"],
        test_case["input_file_path"],
        test_case["expectations_config_path"]
    )


def execute_great_expectations_test_case(title, input_file_path, expectations_config_path):
    """
    This method executes one
    ...
    :return: None. asserts correctness of results.
    """


    # Run the pipeline under test and get the run's output

    out_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_out.csv')
    SuperPipeline().process_file(input_file_path, out_file_path)

    # Load the output dataset into Great Expectations, along with the expectations for this output dataset
    # (must be read from the file - path passed as an argument to this method),
    # and validate the dataset.

    with open(expectations_config_path) as f:
        expectations_config = json.load(f)
    output_df = ge.read_csv(out_file_path, expectations_config=expectations_config)
    output_validation_results = output_df.validate(result_format="SUMMARY", catch_exceptions=True)['results']

    # Pass the validation results to the a method that will assert that the output dataset has met all expectations
    # and will list all the unmet expectations otherwise.

    process_validation_results(output_validation_results)

def process_validation_results(validation_results):
    unmet_validation_results = [expectation_valiation_result for expectation_valiation_result in validation_results if not expectation_valiation_result['success']]
    if len(unmet_validation_results) > 0:
        assert_message = ['\n\n{0:d} expectations out of {1:d} were not met:\n'.format(len(unmet_validation_results), len(validation_results))]
        for unmet_validation_result in unmet_validation_results:
            assert_message.append("{0:s}\n".format(json.dumps(unmet_validation_result, indent=2)))

        assert False, '\n'.join(assert_message)




