import requests
import json
import os
from django.shortcuts import render, redirect
from django.contrib.auth import login, logout, authenticate
from .forms import RegisterForm
from django.contrib import messages
from django.http import JsonResponse
from home.models import User

def register_view(request):
    if request.method == 'POST':
        form = RegisterForm(request.POST)
        if form.is_valid():
            user = form.save(commit=False)
            user.save()
            login(request, user)
            messages.success(request, 'Đăng ký thành công! 🎉')
            return redirect('home')
    else:
        form = RegisterForm()
    return render(request, 'accounts/register.html', {'form': form})


def login_view(request):
    if request.method == 'POST':
        username = request.POST['username']
        password = request.POST['password']
        user = authenticate(request, username=username, password=password)
        if user:
            login(request, user)
            return redirect('home')
        else:
            messages.error(request, 'Tên đăng nhập hoặc mật khẩu không đúng')
    return render(request, 'accounts/login.html')

def social_login_callback(request):
    """Xử lý token từ Google/Facebook và đăng nhập vào Django thông qua Auth Service"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            provider = data.get('provider')
            token = data.get('token')
            
            # Gọi đến Microservice Auth
            auth_url = os.getenv('AUTH_SERVICE_URL', 'http://auth-service:8001')
            endpoint = f"{auth_url}/api/auth/{provider}"
            
            response = requests.post(endpoint, json={"token": token}, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                user_info = result.get('user')
                
                # Tìm hoặc tạo User trong database Django
                user, created = User.objects.get_or_create(
                    email=user_info['email'],
                    defaults={
                        'username': user_info['username'],
                        'name': user_info['full_name'],
                    }
                )
                
                # Đăng nhập vào session Django
                login(request, user)
                return JsonResponse({'status': 'success', 'redirect': '/'})
            else:
                return JsonResponse({
                    'status': 'error', 
                    'message': f"Auth Service Error: {response.text}"
                }, status=400)
                
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
            
    return JsonResponse({'status': 'error', 'message': 'Method not allowed'}, status=405)

def logout_view(request):
    logout(request)
    return redirect('home')