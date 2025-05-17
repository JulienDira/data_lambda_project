from django.urls import path 
from . import views 
urlpatterns = [ 
    path("test_json_view", views.test_json_view, name="test_json_view"),
    path('post_test/', views.post_json_view, name='post_json'), 
    path('get_all_products', views.get_all_products, name='get_all_products'),
    path('most_expensive_product', views.get_most_expensive_product, name='most_expensive_product'),
    path('add_product', views.add_product, name='add_product'),
    path('product/update/<int:product_id>/', views.update_product, name='update_product'),
    path('product/get/pg', views.get_paginated_products, name='get_paginated_products')
] 