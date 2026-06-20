from django.db import models
from django.contrib.auth.models import AbstractUser
from decimal import Decimal
from django.utils import timezone

class User(AbstractUser):
    phone = models.CharField(max_length=15, null=True, blank=True)
    age = models.IntegerField(null=True, blank=True)
    address = models.CharField(max_length=255, null=True, blank=True)
    email = models.EmailField(unique=True)
    balance = models.PositiveIntegerField(default=0)
    name = models.CharField(max_length=255, null=True, blank=True)
    membership_points = models.PositiveIntegerField(default=0)

    def __str__(self):
        return self.name if self.name else self.username


# Model Product
class Product(models.Model):
    CATEGORY_CHOICES = [
        ('food', 'Món ăn'),
        ('drink', 'Nước uống'),
        ('fastfood', 'Thức ăn nhanh'),
    ]

    name = models.CharField(max_length=255)
    image_url = models.URLField(max_length=500)
    description = models.TextField()
    price = models.DecimalField(max_digits=10, decimal_places=2)
    stock_quantity = models.PositiveIntegerField()
    category = models.CharField(max_length=10, choices=CATEGORY_CHOICES, default='food')

    def __str__(self):
        return self.name


# Model Order
class Order(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    total_amount = models.DecimalField(default=Decimal('0.00'), max_digits=10, decimal_places=2)
    discount = models.FloatField(default=0)
    is_paid = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        if self.total_amount > Decimal('800'):
            self.discount = 5
        else:
            self.discount = 0
        super().save(*args, **kwargs)

    def total_after_discount(self):
        return self.total_amount * (Decimal('1.00') - Decimal(self.discount) / Decimal('100'))


# Model OrderItem
class OrderItem(models.Model):
    order = models.ForeignKey(Order, on_delete=models.CASCADE)  
    product = models.ForeignKey(Product, on_delete=models.CASCADE)  
    quantity = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.quantity} x {self.product.name}"


# Model Payment - SỬA LẠI ĐÂY
class Payment(models.Model):
    PAYMENT_METHODS = [
        ('CurrentAccount', 'Tài khoản hiện có'),
        ('Membership', 'Điểm thành viên'),
    ]
    
    STATUS_CHOICES = [
        ('pending', 'Chờ thanh toán'),
        ('completed', 'Đã thanh toán'),
        ('failed', 'Thất bại'),
    ]

    order = models.OneToOneField('Order', on_delete=models.CASCADE)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    payment_method = models.CharField(max_length=20, choices=PAYMENT_METHODS)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='completed')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)  # ✅ THÊM DÒNG NÀY

    def __str__(self):
        return f"Payment for Order #{self.order.id} - {int(self.amount)}K - {self.get_payment_method_display()}"