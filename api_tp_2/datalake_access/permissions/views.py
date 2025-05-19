from rest_framework import viewsets, status, permissions
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from .models import UserPermission
from .serializers import UserPermissionSerializer

# Exiger l'authentification 
class UserPermissionViewSet(viewsets.ModelViewSet):
    queryset = UserPermission.objects.all()
    serializer_class = UserPermissionSerializer
    permission_classes = [IsAuthenticated]

# Pour accorder (grant) une permission
class GrantPermissionView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        serializer = UserPermissionSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# Pour révoquer une permission
class RevokePermissionView(APIView):
    permission_classes = [IsAuthenticated]

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
