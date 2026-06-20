# log_generator.py (sửa đổi)
from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime
import pymysql

# ========================
# KAFKA SETUP - Nhiều topics
# ========================
producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# Định nghĩa các topics
TOPIC_USER = 'user_events'
TOPIC_PRODUCT = 'product_events'
TOPIC_ORDER = 'order_events'
TOPIC_PAYMENT = 'payment_events'

# ========================
# MYSQL CONNECT
# ========================
conn = pymysql.connect(
    host="127.0.0.1",
    user="root",
    password="68686868",
    database="food",
    cursorclass=pymysql.cursors.DictCursor
)

cursor = conn.cursor()

print("🚀 Log Generator (Multiple Topics + HDFS Ready) running...")

# ========================
# DANH SÁCH QUẬN TPHCM
# ========================
DISTRICTS = [
    "Quận 1", "Quận 2", "Quận 3", "Quận 4", "Quận 5", "Quận 6", "Quận 7", 
    "Quận 8", "Quận 9", "Quận 10", "Quận 11", "Quận 12", "Bình Thạnh", 
    "Gò Vấp", "Phú Nhuận", "Tân Bình", "Tân Phú", "Bình Tân", "Thủ Đức",
    "Hóc Môn", "Bình Chánh", "Nhà Bè", "Cần Giờ", "Củ Chi"
]

# ========================
# DANH SÁCH TÊN
# ========================
FIRST_NAMES = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Đặng", "Bùi"]
MIDDLE_NAMES = ["Văn", "Thị", "Đức", "Minh", "Hồng", "Thanh", "Hải", "Tuấn", "Lan", "Hương"]
LAST_NAMES = ["An", "Bình", "Châu", "Dũng", "Hà", "Khánh", "Linh", "Mai", "Nam", "Phong"]

actions = ["view", "add_to_cart", "checkout", "login"]

# ========================
# HÀM INSERT LOG VÀO DATABASE
# ========================
def insert_activity_log(user_id, action, product_id, product_name, quantity, price, status):
    """Insert log vào bảng user_activity_log"""
    try:
        cursor.execute("""
            INSERT INTO user_activity_log 
            (user_id, action, product_id, product_name, quantity, price, status, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            user_id,
            action,
            product_id if product_id else None,
            product_name,
            quantity if quantity else None,
            float(price) if price else None,
            status,
            datetime.now()
        ))
        conn.commit()
    except Exception as db_error:
        print(f"❌ Lỗi insert log: {db_error}")
        conn.rollback()

# ========================
# HÀM TẠO USER ẢO
# ========================
def generate_fake_user(user_id):
    """Tạo user ảo với thông tin ngẫu nhiên"""
    
    first = random.choice(FIRST_NAMES)
    middle = random.choice(MIDDLE_NAMES)
    last = random.choice(LAST_NAMES)
    full_name = f"{first} {middle} {last}"
    
    age = random.randint(18, 70)
    balance = random.randint(0, 5000)
    membership_points = random.randint(0, 8)
    
    district = random.choice(DISTRICTS)
    address = f"{random.randint(1, 500)} {random.choice(['Nguyễn Huệ', 'Lê Lợi', 'Trần Hưng Đạo', 'Phạm Ngũ Lão', 'Hai Bà Trưng'])}, {district}"
    
    phone = f"0{random.randint(3,9)}{random.randint(10000000, 99999999)}"
    email = f"user{user_id}@{random.choice(['gmail.com', 'yahoo.com', 'hotmail.com'])}"
    
    return {
        "id": user_id,
        "username": f"user{user_id}",
        "name": full_name,
        "email": email,
        "phone": phone,
        "age": age,
        "address": address,
        "district": district,
        "balance": balance,
        "membership_points": membership_points
    }

# ========================
# LẤY TẤT CẢ PRODUCT TỪ DB
# ========================
def get_all_products():
    cursor.execute("""
        SELECT id, name, price, stock_quantity, category 
        FROM home_product
    """)
    return cursor.fetchall()

# Cache products
products = get_all_products()
if not products:
    print("❌ No products found")
    exit()

print(f"✅ Loaded {len(products)} products from DB")

# ========================
# HÀM XÁC ĐỊNH TOPIC DỰA VÀO ACTION
# ========================
def get_topic_for_action(action, status, additional_data):
    """
    Xác định topic Kafka dựa vào loại action
    """
    if action == "login":
        return TOPIC_USER
    elif action == "view":
        return TOPIC_PRODUCT
    elif action == "add_to_cart":
        return TOPIC_PRODUCT
    elif action == "checkout":
        if status == "success":
            return TOPIC_ORDER
        else:
            return TOPIC_PAYMENT  # Failed payment
    return TOPIC_USER

# ========================
# MAIN LOOP
# ========================
user_counter = 1
log_count = 0

# Thống kê theo topic
topic_stats = {
    TOPIC_USER: 0,
    TOPIC_PRODUCT: 0,
    TOPIC_ORDER: 0,
    TOPIC_PAYMENT: 0
}

while True:
    try:
        # ========================
        # TẠO USER ẢO
        # ========================
        if random.random() < 0.2:
            user = generate_fake_user(user_counter)
            user_counter += 1
            if user_counter > 50000:
                user_counter = 1
        else:
            old_user_id = random.randint(1, 50000)
            user = generate_fake_user(old_user_id)

        # ========================
        # LẤY PRODUCT NGẪU NHIÊN
        # ========================
        product = random.choice(products)
        
        product_id = product['id']
        product_name = product['name']
        price = float(product['price'])
        stock = product['stock_quantity']
        category = product['category']

        # ========================
        # RANDOM ACTION
        # ========================
        action = random.choice(actions)
        quantity = random.randint(1, 5)
        status = "success"
        additional_data = {}

        # ========================
        # XỬ LÝ THEO ACTION (Giữ nguyên logic cũ)
        # ========================
        
        if action == "view":
            additional_data = {
                "view_duration": random.randint(5, 120),
                "from_page": random.choice(["home", "category", "search"]),
                "device": random.choice(["mobile", "desktop", "tablet"])
            }
            
            insert_activity_log(
                user_id=user["id"],
                action=action,
                product_id=product_id,
                product_name=product_name,
                quantity=None,
                price=price,
                status=status
            )
        
        elif action == "add_to_cart":
            if stock >= quantity:
                additional_data = {
                    "quantity": quantity,
                    "unit_price": price,
                    "subtotal": price * quantity,
                    "available_stock": stock
                }
                status = "success"
            else:
                status = "failed"
                additional_data = {
                    "reason": "Hết hàng",
                    "requested": quantity,
                    "available": stock
                }
            
            insert_activity_log(
                user_id=user["id"],
                action=action,
                product_id=product_id,
                product_name=product_name,
                quantity=quantity,
                price=price,
                status=status
            )
        
        elif action == "checkout":
            subtotal = price * quantity
            discount = 0
            
            if subtotal > 800:
                discount = subtotal * 0.05
            
            final_total = subtotal - discount
            
            payment_method = random.choice(["CurrentAccount", "Membership"])
            
            if payment_method == "Membership":
                points_needed = 6
                if quantity == 1 and user["membership_points"] >= points_needed:
                    user["membership_points"] -= points_needed
                    status = "success"
                    additional_data = {
                        "payment_method": "Membership",
                        "points_used": points_needed,
                        "points_remaining": user["membership_points"],
                        "discount": discount,
                        "original_price": subtotal,
                        "final_price": 0,
                        "items": [{
                            "product_id": product_id,
                            "product_name": product_name,
                            "quantity": quantity,
                            "price": price
                        }]
                    }
                    
                    if stock >= quantity:
                        cursor.execute(
                            "UPDATE home_product SET stock_quantity = stock_quantity - %s WHERE id = %s",
                            (quantity, product_id)
                        )
                        conn.commit()
                    
                    insert_activity_log(
                        user_id=user["id"],
                        action=action,
                        product_id=product_id,
                        product_name=product_name,
                        quantity=quantity,
                        price=price,
                        status=status
                    )
                    
                elif quantity > 1:
                    status = "failed"
                    additional_data = {
                        "payment_method": "Membership",
                        "reason": "Chỉ được dùng điểm khi mua 1 sản phẩm",
                        "quantity": quantity
                    }
                    insert_activity_log(
                        user_id=user["id"],
                        action=action,
                        product_id=product_id,
                        product_name=product_name,
                        quantity=quantity,
                        price=price,
                        status=status
                    )
                else:
                    status = "failed"
                    additional_data = {
                        "payment_method": "Membership",
                        "reason": "Không đủ điểm",
                        "points_required": points_needed,
                        "points_available": user["membership_points"]
                    }
                    insert_activity_log(
                        user_id=user["id"],
                        action=action,
                        product_id=product_id,
                        product_name=product_name,
                        quantity=quantity,
                        price=price,
                        status=status
                    )
                    
            else:  # CurrentAccount
                if user["balance"] >= final_total:
                    user["balance"] -= int(final_total)
                    points_earned = quantity
                    user["membership_points"] += points_earned
                    
                    if stock >= quantity:
                        cursor.execute(
                            "UPDATE home_product SET stock_quantity = stock_quantity - %s WHERE id = %s",
                            (quantity, product_id)
                        )
                        conn.commit()
                    
                    status = "success"
                    additional_data = {
                        "payment_method": "CurrentAccount",
                        "balance_before": user["balance"] + int(final_total),
                        "balance_after": user["balance"],
                        "points_earned": points_earned,
                        "points_after": user["membership_points"],
                        "discount": discount,
                        "subtotal": subtotal,
                        "final_total": final_total,
                        "items": [{
                            "product_id": product_id,
                            "product_name": product_name,
                            "quantity": quantity,
                            "price": price
                        }]
                    }
                    
                    insert_activity_log(
                        user_id=user["id"],
                        action=action,
                        product_id=product_id,
                        product_name=product_name,
                        quantity=quantity,
                        price=price,
                        status=status
                    )
                else:
                    status = "failed"
                    additional_data = {
                        "payment_method": "CurrentAccount",
                        "reason": "Không đủ tiền",
                        "required": int(final_total),
                        "available": user["balance"],
                        "items": [{
                            "product_id": product_id,
                            "product_name": product_name,
                            "quantity": quantity,
                            "price": price
                        }]
                    }
                    
                    insert_activity_log(
                        user_id=user["id"],
                        action=action,
                        product_id=product_id,
                        product_name=product_name,
                        quantity=quantity,
                        price=price,
                        status=status
                    )
        
        elif action == "login":
            additional_data = {
                "login_method": random.choice(["password", "google", "facebook"]),
                "session_id": f"session_{random.randint(10000,99999)}",
                "device": random.choice(["mobile", "desktop", "tablet"]),
                "ip": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,255)}"
            }
            
            insert_activity_log(
                user_id=user["id"],
                action=action,
                product_id=None,
                product_name=None,
                quantity=None,
                price=None,
                status=status
            )

        # ========================
        # BUILD LOG
        # ========================
        log = {
            "log_id": f"LOG{int(time.time())}{random.randint(1000,9999)}",
            "timestamp": datetime.now().isoformat(),
            "user": user,
            "action": action,
            "product": {
                "id": product_id,
                "name": product_name,
                "price": price,
                "category": category,
                "stock_before": stock,
                "stock_after": stock - quantity if action == "checkout" and status == "success" else stock
            } if action in ["view", "add_to_cart", "checkout"] else None,
            "quantity": quantity if action in ["add_to_cart", "checkout"] else None,
            "status": status,
            "additional_data": additional_data,
            "metadata": {
                "source": "log_generator",
                "version": "2.0",
                "user_segment": "vip" if user["balance"] > 2000 else "regular" if user["balance"] > 500 else "new"
            }
        }

        # ========================
        # XÁC ĐỊNH TOPIC VÀ GỬI VÀO KAFKA
        # ========================
        topic = get_topic_for_action(action, status, additional_data)
        producer.send(topic, log)
        
        # Cập nhật thống kê
        topic_stats[topic] += 1
        log_count += 1
        
        # ========================
        # LOG CONSOLE
        # ========================
        status_icon = "✅" if status == "success" else "❌"
        topic_short = topic.replace('_events', '')
        
        if action == "checkout":
            payment_method = additional_data.get("payment_method", "N/A") if status == "success" else additional_data.get("payment_method", additional_data.get("reason", "N/A"))
            print(f"{status_icon} [{topic_short.upper()}] {action.upper()} - User {user['id']} - {product_name} x{quantity} - {payment_method}")
        elif action == "login":
            print(f"🔐 [{topic_short.upper()}] {action.upper()} - User {user['id']} - {user['name']}")
        else:
            print(f"👀 [{topic_short.upper()}] {action.upper()} - User {user['id']} - {product_name} - {status}")

        # In thống kê mỗi 50 logs
        if log_count % 50 == 0:
            print(f"\n📊 TOPIC STATS:")
            for t, count in topic_stats.items():
                t_short = t.replace('_events', '')
                print(f"   {t_short}: {count} messages")
            print(f"   TOTAL: {log_count} messages\n")

        # Random interval
        time.sleep(random.uniform(0.5, 2))

    except Exception as e:
        print("❌ Error:", e)
        import traceback
        traceback.print_exc()
        time.sleep(2)