# forms.py
from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.forms import PasswordChangeForm
from .models import User

User = get_user_model()

class UserProfileForm(forms.ModelForm):
    class Meta:
        model = User
        fields = ['username', 'email', 'phone', 'age', 'address']

class PasswordChangeCustomForm(PasswordChangeForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Customize field labels and help texts
        self.fields['old_password'].label = 'Mật khẩu hiện tại'
        self.fields['new_password1'].label = 'Mật khẩu mới'
        self.fields['new_password2'].label = 'Xác nhận mật khẩu mới'

class DepositForm(forms.Form):
    amount = forms.IntegerField(
        min_value=1000,
        max_value=1000000,
        label='Số tiền nạp (K)',
        help_text='Nhập số tiền từ 1,000K đến 1,000,000K'
    )

class QRCodeUploadForm(forms.Form):
    qr_code = forms.ImageField(
        label='Upload mã QR',
        help_text='Upload ảnh mã QR của bạn (định dạng PNG)'
    )