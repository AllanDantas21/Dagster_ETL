import warnings
import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.validator.validator import Validator
import pandas as pd

warnings.filterwarnings("ignore", category=DeprecationWarning)
df = pd.read_csv('data/sample_data.csv')

# criar um contexto do great_expectations
context = gx.get_context()

# adicionar um data source e um data asset
data_source = context.data_sources.add_pandas("pandas")
data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definion")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

# criar uma expectativa
expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="id", min_value=14, max_value=21
)

# validar a expectativa
validation_result = batch.validate(expectation)

#printar os resultados
print(validation_result.success)