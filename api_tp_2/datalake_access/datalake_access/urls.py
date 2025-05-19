from django.contrib import admin
from django.urls import path, include
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from rest_framework.routers import DefaultRouter
from permissions.views import UserPermissionViewSet, GrantPermissionView, RevokePermissionView

from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi

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
    path('api/advanced/', include('advanced.urls')),
]

schema_view = get_schema_view(
   openapi.Info(
      title="Datalake Access API",
      default_version='v1',
      description="Documentation de l'API du projet de datalake access"
    #   contact=openapi.Contact(email="ton.email@example.com"),
   ),
   public=True,
   permission_classes=(permissions.AllowAny,),
)

urlpatterns += [
   path('swagger/', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
   path('redoc/', schema_view.with_ui('redoc', cache_timeout=0), name='schema-redoc'),
]