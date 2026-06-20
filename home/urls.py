from django.urls import path
from . import views

urlpatterns = [
    path('', views.home_view, name='home'),
    path('profile/', views.user_profile, name='user_profile'),
    path('add-to-cart/<int:product_id>/', views.add_to_cart, name='add_to_cart'),
    path('cart/', views.cart_view, name='cart'),
    path('checkout/', views.checkout, name='checkout'),
    path('decrease/<int:item_id>/', views.decrease_quantity, name='decrease_quantity'),
    path('order-history/', views.order_history, name='order_history'),
    path('user-profile/', views.user_profile, name='user_profile'),
    path('change-password/', views.change_password, name='change_password'),
    path('deposit/', views.deposit_money, name='deposit'),
    path('upload-qr/', views.upload_qr_code, name='upload_qr'),
]