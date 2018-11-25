import pandas as pd
import json
from pyspark.sql.functions import pandas_udf, PandasUDFType

@pandas_udf("dateAdded string, brandCount string", PandasUDFType.GROUPED_MAP)  # doctest: +SKIP
def brand_count_list_udf(key, pdf):
    brand_count_list = [list(x) for x in zip(pdf["brand"],pdf["count"])]
    return pd.DataFrame([key + (json.dumps(brand_count_list),)])