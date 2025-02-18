from great_expectations.validator.validator import Validator
from great_expectations.core.batch import BatchRequest
from dagster import asset, MaterializeResult
import great_expectations as gx
import pandas as pd
import warnings

@asset
def load_data():
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
    expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
        column="age",
        mostly=1
    )

    # validar a expectativa
    validation_result = batch.validate(expectation)

    # Retornar MaterializeResult com metadata  baseada no resultado # DAGSTER
    return MaterializeResult(
        metadata={
            "validation_status": {
                "label": "Validation Status",
                "value": validation_result.success,
                "type": "success" if validation_result.success else "error",
                "unexpected_count": validation_result.result["unexpected_count"]
            }
        }
    )
