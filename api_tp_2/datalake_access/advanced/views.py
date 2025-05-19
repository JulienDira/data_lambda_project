from django.shortcuts import render
import os
import pandas as pd
import pickle
from rest_framework.views import APIView
from rest_framework.response import Response
from whoosh.index import open_dir
from whoosh.qparser import QueryParser
from datetime import datetime
from .utils import push_to_kafka
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from datalake.permissions import CanAccessTablePermission
from rest_framework.permissions import IsAuthenticated

DATALAKE_ROOT = "C:/Users/julie/Documents/Streaming/data_lake"
INDEX_DIR = "index"

class FullTextSearchView(APIView):
    permission_classes = [IsAuthenticated, CanAccessTablePermission]
    def get(self, request):
        keyword = request.query_params.get("q")
        start_date = request.query_params.get("start_date")

        if not keyword or not start_date:
            return Response({"error": "Missing 'q' or 'start_date'"}, status=400)

        ix = open_dir(INDEX_DIR)
        matching_files = []

        with ix.searcher() as searcher:
            query = QueryParser("content", ix.schema).parse(keyword)
            results = searcher.search(query, limit=100)

            for result in results:
                file_path = result['path']
                if f"ingestion_date={start_date}" in file_path:
                    matching_files.append(file_path)

        return Response({"keyword": keyword, "results": matching_files})


class TrainModelRPC(APIView):
    def post(self, request):
        try:
            table_name = request.query_params.get("table")
            ingestion_date = request.query_params.get("ingestion_date")
            
            table_path = os.path.join(DATALAKE_ROOT, table_name, f"ingestion_date={ingestion_date}")
            df = pd.read_parquet(table_path)
            X = df[["AMOUNT", "CUSTOMER_RATING"]]
            y = df["STATUS"]

            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
            model = RandomForestClassifier()
            model.fit(X_train, y_train)
            acc = accuracy_score(y_test, model.predict(X_test))

            with open("model.pkl", "wb") as f:
                pickle.dump(model, f)

            return Response({"status": "Model trained", "accuracy": acc})
        except Exception as e:
            return Response({"error": str(e)}, status=500)


class RepushTransaction(APIView):
    def post(self, request):
        table = request.data.get("table")
        transaction_id = request.data.get("transaction_id")
        ingestion_date = request.data.get("ingestion_date")

        if not table or not transaction_id or not ingestion_date:
            return Response({"error": "Missing parameter"}, status=400)

        path = os.path.join(DATALAKE_ROOT, table, f"ingestion_date={ingestion_date}")
        df = pd.read_parquet(path)
        row = df[df["TRANSACTION_ID"] == transaction_id]

        if row.empty:
            return Response({"error": "Transaction not found"}, status=404)

        record = row.to_dict(orient="records")[0]
        record["TIMESTAMP"] = datetime.utcnow().isoformat()
        push_to_kafka("transactions", record)

        return Response({"status": "Transaction repushed"})


class RepushAll(APIView):
    def post(self, request):
        try:
            for root, dirs, files in os.walk(DATALAKE_ROOT):
                for file in files:
                    if file.endswith(".parquet"):
                        df = pd.read_parquet(os.path.join(root, file))
                        for _, row in df.iterrows():
                            record = row.to_dict()
                            record["TIMESTAMP"] = datetime.utcnow().isoformat()
                            push_to_kafka("transactions", record)
            return Response({"status": "All data repushed to Kafka"})
        except Exception as e:
            return Response({"error": str(e)}, status=500)
