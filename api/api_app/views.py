from django.shortcuts import render
import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.paginator import Paginator
from .models import UserRights
from .models import Product

data = {
   'name': 'John Doe',
   'age': 30,
   'location': 'New York',
   'is_active': True,
}

def has_permission(user, permission_name):
    if not user.is_authenticated:
        return False
    try:
        rights = UserRights.objects.get(user=user)
        return getattr(rights, permission_name, False)
    except UserRights.DoesNotExist:
        return False

# Vue GET existante
def test_json_view(request, data = data):
   if not has_permission(request.user, 'can_view'):
      return JsonResponse({'error': 'Unauthorized: add permission required'}, status=403)
   
   get_data = data.copy()
   return JsonResponse(get_data)

@csrf_exempt
def post_json_view(request, data = data):
   if not has_permission(request.user, 'can_add'):
      return JsonResponse({'error': 'Unauthorized: add permission required'}, status=403)
   
   if request.method == 'POST':
      try:
         body = json.loads(request.body)
         user_name = body.get('user', 'Unknown')
         data = data.copy()
         if user_name == data['name']:
               return JsonResponse(data)
         else:
               return JsonResponse({'error': 'User not found'}, status=404)
      except Exception as e:
         return JsonResponse({'error': f'Invalid JSON: {str(e)}'}, status=400)
   return JsonResponse({'error': 'Only POST allowed'}, status=405)

# Add an endpoint that will return all the products available in the Products table
def get_all_products(request):
   if not has_permission(request.user, 'can_view'):
      return JsonResponse({'error': 'Unauthorized: add permission required'}, status=403)

   products = Product.objects.all()
   product_list = [
      {"name": product.name, "price": product.price, "description": product.description}
      for product in products
   ]
   return JsonResponse({"products": product_list})

# Add an endpoint to retrieve the most expensive product
def get_most_expensive_product(request):
   if not has_permission(request.user, 'can_view'):
      return JsonResponse({'error': 'Unauthorized: add permission required'}, status=403)
   
   try:
      product = Product.objects.order_by('-price').first()  # Trie par prix décroissant et prend le premier
      if product:
         return JsonResponse({
               "name": product.name,
               "price": product.price,
               "description": product.description
         })
      else:
         return JsonResponse({"error": "No products found"}, status=404)
   except Exception as e:
      return JsonResponse({'error': f'Error retrieving product: {str(e)}'}, status=500)
     
# Add an endpoint to add a new product I the table
@csrf_exempt
def add_product(request):
   if not has_permission(request.user, 'can_add'):
      return JsonResponse({'error': 'Unauthorized: add permission required'}, status=403)
   
   if request.method == 'POST':
      try:
         body = json.loads(request.body)
         name = body.get('name')
         price = body.get('price')
         description = body.get('description', "")
         
         if not name or not price:
               return JsonResponse({'error': 'Name and price are required'}, status=400)
         
         # Crée un nouveau produit
         product = Product.objects.create(name=name, price=price, description=description)
         return JsonResponse({'message': f'Product {product.name} created successfully'}, status=201)
      
      except Exception as e:
         return JsonResponse({'error': f'Invalid JSON: {str(e)}'}, status=400)
   
   return JsonResponse({'error': 'Only POST allowed'}, status=405)

# Add an endpoint to update a new product I the table
@csrf_exempt
def update_product(request, product_id):
   if not has_permission(request.user, 'can_update'):
      return JsonResponse({'error': 'Unauthorized: add permission required'}, status=403)
   
   if request.method == 'PUT':  # Utilisation de PUT pour la mise à jour
      try:
         product = Product.objects.get(id=product_id)
         body = json.loads(request.body)
         product.name = body.get('name', product.name)
         product.price = body.get('price', product.price)
         product.description = body.get('description', product.description)
         product.save()
         return JsonResponse({'message': f'Product {product.name} updated successfully'})
      
      except Product.DoesNotExist:
         return JsonResponse({'error': 'Product not found'}, status=404)
      except Exception as e:
         return JsonResponse({'error': f'Invalid JSON: {str(e)}'}, status=400)
   
   return JsonResponse({'error': 'Only PUT allowed'}, status=405)

# Return maximum 3 products at once
def get_paginated_products(request):
   if not has_permission(request.user, 'can_view'):
      return JsonResponse({'error': 'Unauthorized: add permission required'}, status=403)
   
   products = Product.objects.all() 
   paginator = Paginator(products, 3)
   
   page_number = request.GET.get('page', 1)
   page_obj = paginator.get_page(page_number)
   
   product_list = [{"name": product.name, "price": product.price, "description": product.description} for product in page_obj]
   
   return JsonResponse({"products": product_list, "num_pages": paginator.num_pages})








