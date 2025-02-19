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

    collumns = df.columns.tolist()
    table_error = {}

    # criar uma expectativa
    for coll_name in collumns:
        expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
            column=coll_name,
            mostly=1
        )

        # validar a expectativa
        validation_result = batch.validate(expectation)
        if validation_result.success is False:
            percent = validation_result.result["unexpected_percent"]
            body_error = { "name_collumn": coll_name, "percent": f"{percent:.2f}%",
                            "total_elements":validation_result.result["element_count"],
                            "total_error_count":validation_result.result["unexpected_count"]}
            if body_error["name_collumn"] not in table_error:
                table_error[coll_name] = []
            table_error[coll_name].append(body_error)
    
    if len(table_error) != 0:
        print(f" {len(table_error)} colunas não alcançaram as espectativas de um total de {len(collumns)}")
    else:
        print("Deu bom")

    return MaterializeResult(metadata = {
        "validation_status": table_error
    })
    # # Retornar MaterializeResult com metadata  baseada no resultado # DAGSTER
    # return MaterializeResult(
    #     metadata={
    #         "validation_status": {
    #             "label": "Validation Status",
    #             "value": validation_result.success,
    #             "type": "success" if validation_result.success else "error",
    #             "unexpected_percent": f"{percent:.2f}%"
    #         }
    #     }
    # )
