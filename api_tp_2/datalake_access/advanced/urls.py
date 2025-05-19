from django.urls import path
from .views import (
    FullTextSearchView,
    TrainModelRPC
    RepushTransaction,
    RepushAll
)

urlpatterns = [
    path('search/fulltext/', FullTextSearchView.as_view(), name='fulltext-search'),
    path('rpc/train-model/', TrainModelRPC.as_view(), name='train-model'),
    path('repush/transaction/', RepushTransaction.as_view(), name='repush-transaction'),
    path('repush/all/', RepushAll.as_view(), name='repush-all'),
]
