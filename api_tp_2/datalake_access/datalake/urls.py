from django.urls import path
from .views import RetrieveTableView

urlpatterns = [
    path('table/', RetrieveTableView.as_view(), name='retrieve_table'),
]
