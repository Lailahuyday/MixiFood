from django.shortcuts import render, redirect, get_object_or_404
from .models import Payment, Product, Order, OrderItem
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from decimal import Decimal
from django.db.models import Sum
from .forms import UserProfileForm, PasswordChangeCustomForm, DepositForm, QRCodeUploadForm
from django.contrib.auth import update_session_auth_hash
import os
from django.conf import settings

def home_view(request):
    foods = Product.objects.filter(category='food')
    drinks = Product.objects.filter(category='drink')
    fastfoods = Product.objects.filter(category='fastfood')

    context = {
        'foods': foods,
        'drinks': drinks,
        'fastfoods': fastfoods,
    }
    return render(request, 'home/home.html', context)

@login_required
def user_profile(request):
    if request.method == 'POST':
        profile_form = UserProfileForm(request.POST, instance=request.user)
        if profile_form.is_valid():
            profile_form.save()
            messages.success(request, "Thông tin cá nhân đã được cập nhật!")
            return redirect('user_profile')
    else:
        profile_form = UserProfileForm(instance=request.user)
    
    return render(request, 'home/user_profile.html', {
        'form': profile_form
    })

def add_to_cart(request, product_id):
    user = request.user
    product = get_object_or_404(Product, id=product_id)

    # ✅ Kiểm tra sản phẩm hết hàng
    if product.stock_quantity <= 0:
        messages.error(request, f"Xin lỗi, {product.name} hiện đã hết hàng.")
        return redirect('home')  # hoặc dùng request.META.get('HTTP_REFERER')

    # Tìm giỏ hàng chưa thanh toán
    order = Order.objects.filter(user=user, is_paid=False).first()

    if not order:
        order = Order.objects.create(user=user, is_paid=False, total_amount=0)

    # Kiểm tra sản phẩm đã trong giỏ chưa
    order_item = OrderItem.objects.filter(order=order, product=product).first()

    if order_item:
        # ✅ Không cho vượt quá số lượng tồn kho
        if order_item.quantity < product.stock_quantity:
            order_item.quantity += 1
            order_item.save()
        else:
            messages.error(request, f"Số lượng {product.name} trong kho chỉ còn {product.stock_quantity}.")
            return redirect('cart')
    else:
        OrderItem.objects.create(order=order, product=product, quantity=1)

    # Cập nhật tổng tiền
    total = sum(item.product.price * item.quantity for item in order.orderitem_set.all())
    order.total_amount = total
    order.save()

    messages.success(request, f"Đã thêm {product.name} vào giỏ hàng!")
    return redirect('cart')

@login_required
def cart_view(request):
    user = request.user
    order = Order.objects.filter(user=user, is_paid=False).first()
    order_items = order.orderitem_set.all() if order else []

    # Sử dụng Decimal để tính toán tổng tiền
    total = sum(Decimal(item.product.price) * Decimal(item.quantity) for item in order_items)

    discount = Decimal(0)
    if total > Decimal(800):  # Giảm giá 5% nếu tổng tiền lớn hơn 800K
        discount = total * Decimal(0.05)

    final_total = total - discount

    # Lấy số điểm từ người dùng
    membership_points = user.membership_points  # Sử dụng trường membership_points thay vì points

    # Tính số sản phẩm cần mua thêm để đạt đủ 6 điểm
    products_left = 6 - membership_points if membership_points < 6 else 0

    context = {
        'order_items': order_items,
        'total': total,
        'discount': discount,
        'final_total': final_total,
        'products_left': products_left,  # Truyền giá trị products_left vào context
        'membership_points': membership_points,  # Truyền membership_points vào context
    }

    return render(request, 'home/cart.html', context)

def decrease_quantity(request, item_id):
    item = get_object_or_404(OrderItem, id=item_id)

    if item.quantity > 1:
        item.quantity -= 1
        item.save()
        messages.success(request, f"Đã giảm số lượng món {item.product.name}.")
    else:
        item.delete()
        messages.success(request, f"Đã xoá món {item.product.name} khỏi giỏ hàng.")

    return redirect('cart')  

@login_required
def checkout(request):
    user = request.user
    order = Order.objects.filter(user=user, is_paid=False).first()

    if not order:
        messages.error(request, "Bạn chưa có đơn hàng nào.")
        return redirect('cart')

    if order.is_paid:
        messages.error(request, "Đơn hàng này đã được thanh toán.")
        return redirect('home')

    order_items = order.orderitem_set.all()

    # Tính tổng tiền đơn hàng
    total = sum(item.product.price * item.quantity for item in order_items)

    discount = Decimal('0')
    if total > Decimal('800'):
        discount = total * Decimal('0.05')

    # Kiểm tra điều kiện sử dụng điểm
    membership_used = False
    membership_discount = Decimal('0')
    payment_method = request.POST.get('payment_method')

    if payment_method == 'Membership':
        if user.membership_points >= 6:
            if len(order_items) == 1 and order_items[0].quantity == 1:
                cheapest_item = order_items[0]
                membership_discount = cheapest_item.product.price
                user.membership_points -= 6
                user.save()
                membership_used = True
            else:
                messages.error(request, "Bạn chỉ có thể dùng điểm khi mua 1 sản phẩm duy nhất với số lượng = 1!")
                return redirect('cart')
        else:
            messages.error(request, "Bạn không đủ điểm để thanh toán bằng Membership!")
            return redirect('cart')

    final_total = total - discount - membership_discount

    # Nếu không phải Membership thì phải kiểm tra và trừ tiền từ tài khoản hiện có
    if payment_method == 'CurrentAccount':
        if user.balance < final_total:
            messages.error(request, "⚠️ Không đủ tiền trong tài khoản!")
            return redirect('cart')
        user.balance -= int(final_total)
        user.save()

    # Trừ hàng tồn kho
    for item in order_items:
        if item.product.stock_quantity >= item.quantity:
            item.product.stock_quantity -= item.quantity
            item.product.save()
        else:
            messages.error(request, f"Món {item.product.name} không đủ hàng!")
            return redirect('cart')

    # Ghi lại thanh toán
    Payment.objects.create(
        order=order,
        amount=final_total,
        payment_method=payment_method
    )

    order.is_paid = True
    order.save()

    # Cộng điểm nếu thanh toán bằng 'CurrentAccount'
    if payment_method == 'CurrentAccount':
        total_quantity = sum(item.quantity for item in order_items)
        user.membership_points += total_quantity
        user.save()

    # Tạo message thông báo
    if payment_method == 'Membership':
        msg = f"✅ Thanh toán thành công! Đã dùng 6 điểm Membership để miễn phí món '{cheapest_item.product.name}' ({int(membership_discount)}K) 🎉"
    else:
        msg = f"✅ Thanh toán thành công! Đã trừ {int(final_total)}K từ tài khoản 🎉"
        if discount > 0:
            msg += f" (Giảm giá {int(discount)}K)"

    messages.success(request, msg)

    return redirect('home')

def order_history(request):
    # Lọc các đơn hàng đã thanh toán của người dùng
    orders = Order.objects.filter(user=request.user, is_paid=True).prefetch_related('orderitem_set__product')

    # Kiểm tra nếu không có đơn hàng nào đã thanh toán
    if not orders:
        messages.info(request, "Bạn chưa có đơn hàng nào đã thanh toán.")
    
    # Lấy phương thức thanh toán cho từng đơn hàng
    payments = Payment.objects.filter(order__in=orders)

    # Tính tổng tiền đã thanh toán cho mỗi đơn hàng
    for order in orders:
        order.total_to_pay = order.total_after_discount()

    # Gửi dữ liệu vào template
    return render(request, 'home/order_history.html', {'orders': orders, 'payments': payments})

@login_required
def deposit_money(request):
    if request.method == 'POST':
        form = DepositForm(request.POST)
        if form.is_valid():
            amount = form.cleaned_data['amount']
            user = request.user
            user.balance += amount
            user.save()
            messages.success(request, f'✅ Nạp tiền thành công! Số dư hiện tại: {user.balance}K')
            return redirect('user_profile')
    else:
        form = DepositForm()
    return render(request, 'home/deposit.html', {'form': form})

@login_required
def change_password(request):
    if request.method == 'POST':
        form = PasswordChangeCustomForm(request.user, request.POST)
        if form.is_valid():
            user = form.save()
            update_session_auth_hash(request, user)
            messages.success(request, '✅ Mật khẩu đã được thay đổi thành công!')
            return redirect('user_profile')
    else:
        form = PasswordChangeCustomForm(request.user)
    return render(request, 'home/change_password.html', {'form': form})

@login_required
def upload_qr_code(request):
    if request.method == 'POST':
        form = QRCodeUploadForm(request.POST, request.FILES)
        if form.is_valid():
            qr_code = request.FILES['qr_code']
            # Tạo thư mục nếu chưa tồn tại
            upload_dir = os.path.join(settings.BASE_DIR, 'static', 'home', 'images')
            os.makedirs(upload_dir, exist_ok=True)
            
            # Lưu file vào thư mục static
            file_path = os.path.join(upload_dir, 'qr-code.png')
            with open(file_path, 'wb+') as destination:
                for chunk in qr_code.chunks():
                    destination.write(chunk)
            messages.success(request, '✅ Mã QR đã được cập nhật thành công!')
            return redirect('deposit')
    else:
        form = QRCodeUploadForm()
    return render(request, 'home/upload_qr.html', {'form': form})

