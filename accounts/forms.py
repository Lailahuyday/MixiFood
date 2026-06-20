from django import forms
from django.contrib.auth.forms import UserCreationForm
from home.models import User  # hoặc từ model chứa User nếu bạn di chuyển User qua accounts sau này

class RegisterForm(UserCreationForm):
    email = forms.EmailField(required=True)
    phone = forms.CharField(required=False)
    age = forms.IntegerField(required=False)
    address = forms.CharField(required=False)

    class Meta:
        model = User
        fields = ['username', 'email', 'password1', 'password2', 'phone', 'age', 'address']
