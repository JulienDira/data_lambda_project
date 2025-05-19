# permissions/urls.py
from django.urls import path
from rest_framework.routers import DefaultRouter
from .views import UserPermissionViewSet

router = DefaultRouter()
router.register(r'', UserPermissionViewSet, basename='user-permission')

urlpatterns = router.urls
