# How to write integration tests for data pipelines using Great Expectations and pytest

### This repo contains an example of a data pipeline integration test and explanation of how to adjust it to your needs.

[Great Expectations](https://github.com/great-expectations/great_expectations) is an open source library that allows the writing of declarative 
statements about what data should look like. Expectations can range from simple statements such as “expect column X to 
exist” to sophisticated statements that reason about distribution of values, their uniqueness and types. 
Great Expectations provides an extensive library of expectation types. Users can fully extend it with their custom 
logic (e.g., “expect values to be internal employee numbers”).

You can use Great Expectations to put your data pipeline under test using clear declarative style that makes it easy to 
express yourself, communicate with team mates and analyze errors.  
 
For the purpose of integration tests your data pipeline can be treated as a "black box". You prepare an input dataset 
for each test case and use Great Expectations to state what you
expect the pipeline's output to look like. Then you run the input through the data pipeline under test and use 
Great Expectations to validate the pipeline's output against the test case's expectations.


The example in this repo defines a trivial data pipeline [super_pipeline.py](super_pipeline.py) that expects a CSV file
as its input and produces a CSV file as its output. 
 
Instead of implementing test cases in Python, we define them in a [JSON file](test/expectation_configs/expectation_config_1.json):

```
{
  "test_cases" : [
    {
      "title": "GE testcase 1",
      "input_file_path": "input_datasets/test_input_1.csv",
      "expectations_config_path": "expectations_configs/expectations_config_1.json"
    }
    ]
}

```
The exact attributes in the test case configuration are up to you, since your pipeline inputs may be different from
the one in the example. The "input_file_path" attribute in the example above assumes that the pipeline under test takes 
one file as its input. Your pipeline might require multiple files or not rely on files at all, using query parameters 
or some other ways to get inputs. 

[test_super_pipeline.py](test/test_super_pipeline.py) contains the test runner implementation. 
It reads our test case configuration file and uses [pytest parametrization](https://docs.pytest.org/en/latest/parametrize.html#pytest-generate-tests)
to turn each test case into a pytest test case. See comments in the file for explanation of how to customize it for 
your pipeline.
 
The test uses Great Expectations' [validate](https://great-expectations.readthedocs.io/en/latest/validation.html?highlight=validate)
to check if the pipeline's output after running on the test case's input meets your expectations. If not all expectations
are met, the test will fail and make sure that all failing expectations will be displayed in the assert message:



```
>           assert False, '\n'.join(assert_message)
E           AssertionError: 
E             
E             4 expectations out of 6 were not met:
E             
E             {
E               "success": false,
E               "exception_info": {
E                 "raised_exception": false,
E                 "exception_message": null,
E                 "exception_traceback": null
E               },
...
```
