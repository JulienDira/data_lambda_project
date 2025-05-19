# datalake/views.py
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
import os
import pandas as pd
import numpy as np
import pyarrow.dataset as ds
from datalake.permissions import CanAccessTablePermission

DATALAKE_ROOT = "C:/Users/julie/Documents/Streaming/data_lake"

def clean_df_for_json(df: pd.DataFrame) -> pd.DataFrame:
    # Remplacer inf et -inf par NaN
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Convertir datetime en string ISO 8601 (en remplaçant NaT par None)
    for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']):
        df[col] = df[col].astype(str).replace('NaT', None)

    # Forcer le type object pour pouvoir mettre None
    df = df.astype(object)

    # Remplacer NaN par None
    df = df.where(pd.notnull(df), None)

    return df

class RetrieveTableView(APIView):
    permission_classes = [IsAuthenticated, CanAccessTablePermission]

    def get(self, request):
        table_name = request.query_params.get("table")
        columns = request.query_params.getlist("columns")
        page = int(request.query_params.get("page", 1))

        if not table_name:
            return Response({"error": "Missing 'table' parameter"}, status=400)

        table_path = os.path.join(DATALAKE_ROOT, table_name)
        if not os.path.exists(table_path) or not os.path.isdir(table_path):
            return Response({"error": f"Table '{table_name}' not found"}, status=404)

        try:
            dataset = ds.dataset(table_path, format="parquet", partitioning="hive")
            df = dataset.to_table().to_pandas()

            if columns:
                invalid_cols = [col for col in columns if col not in df.columns]
                if invalid_cols:
                    return Response({"error": f"Invalid column(s) requested: {invalid_cols}"}, status=400)
                df = df[columns]

            page_size = 10
            start = (page - 1) * page_size
            end = start + page_size
            page_df = df.iloc[start:end]

            # Nettoyage avant conversion JSON
            page_df = clean_df_for_json(page_df)

            paginated_data = page_df.to_dict(orient="records")

            return Response({
                "table": table_name,
                "page": page,
                "total_rows": len(df),
                "data": paginated_data
            })

        except Exception as e:
            return Response({"error": str(e)}, status=500)
