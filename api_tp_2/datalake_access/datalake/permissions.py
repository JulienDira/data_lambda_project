from rest_framework.permissions import BasePermission
from django.db import connection

class CanAccessTablePermission(BasePermission):
    message = "Vous n'avez pas la permission d'accéder à cette table."

    def has_permission(self, request, view):
        table_name = request.query_params.get("table")  # e.g. "TRANSACTIONS_IMPORTANTES_700"
        user = request.user

        if not user.is_authenticated or not table_name:
            return False

        folder_match = f"%{table_name}%"

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1 FROM user_permissions
                WHERE user_id = %s
                  AND folder_path ILIKE %s
                  AND permission_type = 'read'
                """,
                [user.id, folder_match]
            )
            result = cursor.fetchone()

        return result is not None
