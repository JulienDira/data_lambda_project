from rest_framework import serializers
from .models import UserPermission

class UserPermissionSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserPermission
        fields = ['user', 'username', 'folder_path', 'permission_type', 'created_at', 'updated_at']
        read_only_fields = ['username', 'created_at', 'updated_at']  # username auto rempli

    def create(self, validated_data):
        user = validated_data.get('user')
        validated_data['username'] = user.username
        return super().create(validated_data)
