from django.urls import path
from .views import RetrieveTableView, MetricsView

urlpatterns = [
    path('table/', RetrieveTableView.as_view(), name='retrieve_table'),
    path('metrics/', MetricsView.as_view(), name='metrics')
]
