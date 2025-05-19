from datetime import datetime, timedelta
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
import os
import pandas as pd
import numpy as np
import pyarrow.dataset as ds
from datalake.permissions import CanAccessTablePermission
import pytz
import logging

# Swagger imports
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

logger = logging.getLogger("access")
DATALAKE_ROOT = "C:/Users/julie/Documents/Streaming/data_lake"

def clean_df_for_json(df: pd.DataFrame) -> pd.DataFrame:
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']):
        df[col] = df[col].astype(str).replace('NaT', None)
    df = df.astype(object)
    df = df.where(pd.notnull(df), None)
    return df

class RetrieveTableView(APIView):
    permission_classes = [IsAuthenticated, CanAccessTablePermission]

    @swagger_auto_schema(
        operation_description="Retrieve a specific page of filtered data from a given table and ingestion_date.",
        manual_parameters=[
            openapi.Parameter('table', openapi.IN_QUERY, description="Table name", type=openapi.TYPE_STRING, required=True),
            openapi.Parameter('columns', openapi.IN_QUERY, description="List of columns to return", type=openapi.TYPE_ARRAY, items=openapi.Items(type=openapi.TYPE_STRING)),
            openapi.Parameter('page', openapi.IN_QUERY, description="Page number (default: 1)", type=openapi.TYPE_INTEGER),
            openapi.Parameter('ingestion_date', openapi.IN_QUERY, description="Ingestion date", type=openapi.TYPE_STRING),
            openapi.Parameter('PAYMENT_METHOD', openapi.IN_QUERY, description="Filter by payment method", type=openapi.TYPE_STRING),
            openapi.Parameter('COUNTRY', openapi.IN_QUERY, description="Filter by country", type=openapi.TYPE_STRING),
            openapi.Parameter('PRODUCT_CATEGORY', openapi.IN_QUERY, description="Filter by product category", type=openapi.TYPE_STRING),
            openapi.Parameter('STATUS', openapi.IN_QUERY, description="Filter by status", type=openapi.TYPE_STRING),
            openapi.Parameter('AMOUNT_eq', openapi.IN_QUERY, description="Filter AMOUNT equal to", type=openapi.TYPE_NUMBER),
            openapi.Parameter('AMOUNT_gt', openapi.IN_QUERY, description="Filter AMOUNT greater than", type=openapi.TYPE_NUMBER),
            openapi.Parameter('AMOUNT_lt', openapi.IN_QUERY, description="Filter AMOUNT less than", type=openapi.TYPE_NUMBER),
            openapi.Parameter('CUSTOMER_RATING_eq', openapi.IN_QUERY, description="Filter CUSTOMER_RATING equal to", type=openapi.TYPE_NUMBER),
            openapi.Parameter('CUSTOMER_RATING_gt', openapi.IN_QUERY, description="Filter CUSTOMER_RATING greater than", type=openapi.TYPE_NUMBER),
            openapi.Parameter('CUSTOMER_RATING_lt', openapi.IN_QUERY, description="Filter CUSTOMER_RATING less than", type=openapi.TYPE_NUMBER),
        ],
        responses={200: "Paginated and filtered data", 400: "Bad Request", 404: "Not Found"}
    )
    def get(self, request):
        table_name = request.query_params.get("table")
        columns = request.query_params.getlist("columns")
        page = int(request.query_params.get("page", 1))
        ingestion_date = request.query_params.get("ingestion_date")

        logger.info(f"User {request.user.username} accessed table {table_name} at {datetime.utcnow().isoformat()}")

        if not table_name:
            return Response({"error": "Missing 'table' parameter"}, status=400)

        table_path = os.path.join(DATALAKE_ROOT, table_name, f"ingestion_date={ingestion_date}")
        if not os.path.exists(table_path) or not os.path.isdir(table_path):
            return Response({
                "error": f"Table '{table_name}' and ingestion_date {ingestion_date} not found", 
                "path": table_path
            }, status=404)

        try:
            dataset = ds.dataset(table_path, format="parquet", partitioning="hive")
            df = dataset.to_table().to_pandas()

            filters = {
                "PAYMENT_METHOD": request.query_params.get("PAYMENT_METHOD"),
                "COUNTRY": request.query_params.get("COUNTRY"),
                "PRODUCT_CATEGORY": request.query_params.get("PRODUCT_CATEGORY"),
                "STATUS": request.query_params.get("STATUS"),
            }

            for key, value in filters.items():
                if value and key in df.columns:
                    df = df[df[key] == value]

            for field in ["AMOUNT", "CUSTOMER_RATING"]:
                if field in df.columns:
                    val_eq = request.query_params.get(f"{field}_eq")
                    val_gt = request.query_params.get(f"{field}_gt")
                    val_lt = request.query_params.get(f"{field}_lt")

                    if val_eq:
                        df = df[df[field] == float(val_eq)]
                    if val_gt:
                        df = df[df[field] > float(val_gt)]
                    if val_lt:
                        df = df[df[field] < float(val_lt)]

            if columns:
                invalid_cols = [col for col in columns if col not in df.columns]
                if invalid_cols:
                    return Response({"error": f"Invalid column(s) requested: {invalid_cols}"}, status=400)
                df = df[columns]

            page_size = 10
            start = (page - 1) * page_size
            end = start + page_size
            page_df = df.iloc[start:end]

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


class MetricsView(APIView):
    permission_classes = [IsAuthenticated, CanAccessTablePermission]

    @swagger_auto_schema(
        operation_description="Retrieve metrics (recent amount spent, totals per user/type, top products) from a table.",
        manual_parameters=[
            openapi.Parameter('table', openapi.IN_QUERY, description="Table name", type=openapi.TYPE_STRING, required=True),
            openapi.Parameter('x', openapi.IN_QUERY, description="Top X products (default: 5)", type=openapi.TYPE_INTEGER),
        ],
        responses={200: "Metrics returned", 400: "Missing parameter", 404: "Table not found"}
    )
    def get(self, request):

        table_name = request.query_params.get("table")
        top_x = int(request.query_params.get("x", 5))

        logger.info(f"User {request.user.username} accessed table {table_name} at {datetime.utcnow().isoformat()}")

        if not table_name:
            return Response({"error": "Missing 'table' parameter"}, status=400)

        table_path = os.path.join(DATALAKE_ROOT, table_name)
        if not os.path.exists(table_path):
            return Response({"error": f"Table '{table_name}' not found"}, status=404)

        try:
            dataset = ds.dataset(table_path, format="parquet", partitioning="hive")
            df = dataset.to_table().to_pandas()

            now = datetime.utcnow().replace(tzinfo=pytz.UTC)
            five_minutes_ago = now - timedelta(minutes=5)

            if "TIMESTAMP" in df.columns:
                df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
                recent_df = df[df["TIMESTAMP"] >= five_minutes_ago]
                recent_spent = recent_df["AMOUNT"].sum()
            else:
                recent_spent = None

            total_per_user_type = []
            if {"USER_ID_HASHED", "TRANSACTION_TYPE", "AMOUNT"}.issubset(df.columns):
                grouped = df.groupby(["USER_ID_HASHED", "TRANSACTION_TYPE"])["AMOUNT"].sum().reset_index()
                total_per_user_type = grouped.to_dict(orient="records")

            top_products = []
            if "PRODUCT_ID" in df.columns:
                top = (
                    df["PRODUCT_ID"]
                    .value_counts()
                    .head(top_x)
                    .reset_index()
                    .rename(columns={"index": "product_name", "product_name": "count"})
                )
                top_products = top.to_dict(orient="records")

            return Response({
                "recent_spent": recent_spent,
                "total_per_user_type": total_per_user_type,
                "top_products": top_products,
            })

        except Exception as e:
            return Response({"error": str(e)}, status=500)


class ListResourcesView(APIView):
    permission_classes = [IsAuthenticated]

    @swagger_auto_schema(
        operation_description="List all available resources (tables and partitions) in the Datalake.",
        responses={200: "Resources listed", 500: "Server error"}
    )
    def get(self, request):
        try:
            resources = []
            for root, dirs, files in os.walk(DATALAKE_ROOT):
                for dir in dirs:
                    full_path = os.path.join(root, dir)
                    relative_path = os.path.relpath(full_path, DATALAKE_ROOT)
                    resources.append(relative_path)
            return Response({"resources": resources})
        except Exception as e:
            return Response({"error": str(e)}, status=500)
