# admin.py

from django.contrib import admin
from .models import User, Product, Order, OrderItem, Payment

class UserAdmin(admin.ModelAdmin):
    list_display = ('username', 'name', 'email', 'balance')  
    search_fields = ('username', 'name', 'email')  

admin.site.register(User, UserAdmin)
admin.site.register(Product)
admin.site.register(Order)
admin.site.register(OrderItem)
admin.site.register(Payment)
