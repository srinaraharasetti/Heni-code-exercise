Nutter Fixture
The Nutter Runner is simply a base Python class, NutterFixture, that test fixtures implement. The runner runtime is a module you can use once you install Nutter on the Databricks cluster. The NutterFixture base class can then be imported in a test notebook and implemented by a test fixture:

from runtime.nutterfixture import NutterFixture, tag
class MyTestFixture(NutterFixture):
   …
To run the tests:

result = MyTestFixture().execute_tests()
To view the results from within the test notebook:

print(result.to_string())
To return the test results to the Nutter CLI:

result.exit(dbutils)

Note: The call to result.exit, behind the scenes calls dbutils.notebook.exit, passing the serialized TestResults back to the CLI. At the current time, print statements do not work when dbutils.notebook.exit is called in a notebook, even if they are written prior to the call. For this reason, it is required to temporarily comment out result.exit(dbutils) when running the tests locally.
