from django.db import models
from django.contrib.auth.models import User

# Model pour accorder des permissions et stocker cela dans la table postgres précédemment créée
class UserPermission(models.Model):
    permission_id = models.AutoField(primary_key=True)
    PERMISSION_CHOICES = [
        ('read', 'Read'),
        ('write', 'Write'),
        ('execute', 'Execute'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    username = models.CharField(max_length=150, null=True, blank=True)  # temporairement nullable
    folder_path = models.TextField()
    permission_type = models.CharField(max_length=10, choices=PERMISSION_CHOICES)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        # Remplir automatiquement username à partir de l'objet user
        if self.user and not self.username:
            self.username = self.user.username
        super().save(*args, **kwargs)

    class Meta:
        db_table = 'user_permissions'
        unique_together = ('user', 'folder_path')
