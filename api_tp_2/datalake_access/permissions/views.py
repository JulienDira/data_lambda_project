from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from .models import UserPermission
from .serializers import UserPermissionSerializer

# Swagger
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

# Vue standard CRUD des permissions
class UserPermissionViewSet(viewsets.ModelViewSet):
    queryset = UserPermission.objects.all()
    serializer_class = UserPermissionSerializer
    permission_classes = [IsAuthenticated]


# Pour accorder une permission
class GrantPermissionView(APIView):
    permission_classes = [IsAuthenticated]

    @swagger_auto_schema(
        operation_description="Grant access permission to a user for a specific folder path.",
        request_body=UserPermissionSerializer,
        responses={
            201: openapi.Response("Permission granted", UserPermissionSerializer),
            400: "Invalid input data"
        }
    )
    def post(self, request):
        serializer = UserPermissionSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


# Pour révoquer une permission
class RevokePermissionView(APIView):
    permission_classes = [IsAuthenticated]

    @swagger_auto_schema(
        operation_description="Revoke access permission for a user and folder path.",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            required=["user", "folder_path"],
            properties={
                "user": openapi.Schema(type=openapi.TYPE_INTEGER, description="User ID"),
                "folder_path": openapi.Schema(type=openapi.TYPE_STRING, description="Folder path"),
            }
        ),
        responses={
            200: "Permission revoked",
            400: "Missing user or folder_path",
            404: "Permission not found"
        }
    )
    def post(self, request):
        user_id = request.data.get("user")
        folder_path = request.data.get("folder_path")

        if not user_id or not folder_path:
            return Response(
                {"error": "user and folder_path are required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            permission = UserPermission.objects.get(user_id=user_id, folder_path=folder_path)
            permission.delete()
            return Response({"message": "Permission revoked"}, status=status.HTTP_200_OK)
        except UserPermission.DoesNotExist:
            return Response({"error": "Permission not found"}, status=status.HTTP_404_NOT_FOUND)
