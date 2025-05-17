from django.contrib import admin
from .models import Product, UserRights

# Register your models here.
admin.site.register(Product)
admin.site.register(UserRights)