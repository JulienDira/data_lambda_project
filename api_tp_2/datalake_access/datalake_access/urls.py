from django.contrib import admin
from django.urls import path, include
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from rest_framework.routers import DefaultRouter
from permissions.views import UserPermissionViewSet, GrantPermissionView, RevokePermissionView


# Router pour le CRUD automatique des permissions
router = DefaultRouter()
router.register(r'permissions', UserPermissionViewSet)

urlpatterns = [
    # Admin
    path('admin/', admin.site.urls),

    # JWT - Authentification
    path('api/token/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('api/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),

    # Endpoints CRUD auto (list, create, update, delete)
    path('api/', include(router.urls)),

    # Endpoints spécifiques pour grant/revoke permissions
    path('api/grant/', GrantPermissionView.as_view(), name='grant-permission'), # Ajouter des permissions
    path('api/revoke/', RevokePermissionView.as_view(), name='revoke-permission'), # Supprimer des permissions
    
    path('api/datalake/', include('datalake.urls')), 
]
