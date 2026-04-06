import random
import os
import schedule
import time
import threading
import psycopg2
from datetime import datetime, date
from psycopg2.extras import RealDictCursor
from mattermostdriver import Driver
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

class Database:
    def __init__(self):
        self.conn = None
        
    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                dbname=os.getenv('DB_NAME', 'birthday_bot'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD')
            )
            self.create_tables()
        except Exception as e:
            print(f"Ошибка подключения к БД: {e}")
            raise
            
    def create_tables(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(100) NOT NULL UNIQUE,
                    full_name VARCHAR(200) NOT NULL,
                    gender VARCHAR(10) NOT NULL CHECK (gender IN ('male', 'female')),
                    birth_date DATE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_by VARCHAR(100)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_birth_date ON users(birth_date)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_gender ON users(gender)
            """)
            self.conn.commit()
            
    def add_user(self, username, full_name, gender, birth_date, created_by):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (username, full_name, gender, birth_date, created_by)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (username) DO UPDATE SET
                    full_name = EXCLUDED.full_name,
                    gender = EXCLUDED.gender,
                    birth_date = EXCLUDED.birth_date,
                    created_by = EXCLUDED.created_by
            """, (username, full_name, gender, birth_date, created_by))
            self.conn.commit()
            
    def remove_user(self, username):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE username = %s", (username,))
            self.conn.commit()
            return cur.rowcount > 0
            
    def get_all_users(self):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, username, full_name, gender, birth_date FROM users ORDER BY username")
            return cur.fetchall()
            
    def get_today_birthdays(self):
        today = date.today()
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, username, full_name, gender, birth_date 
                FROM users 
                WHERE EXTRACT(MONTH FROM birth_date) = %s 
                AND EXTRACT(DAY FROM birth_date) = %s
            """, (today.month, today.day))
            return cur.fetchall()
            
    def get_user_by_username(self, username):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE username = %s", (username,))
            return cur.fetchone()
            
    def close(self):
        if self.conn:
            self.conn.close()

class BirthdayBot:
    def __init__(self):
        self.driver = Driver({
            'url': os.getenv('MATTERMOST_URL'),
            'token': os.getenv('MATTERMOST_TOKEN'),
            'scheme': 'http' if 'http://' in os.getenv('MATTERMOST_URL') else 'https',
            'verify': False
        })
        self.db = Database()
        self.db.connect()
        self.admin_channel_id = None
        self.notification_channel_id = None
        
        # Пути к файлам
        self.base_dir = os.path.dirname(__file__)
        self.images_male_dir = os.path.join(self.base_dir, 'images', 'male')
        self.images_female_dir = os.path.join(self.base_dir, 'images', 'female')
        self.congrats_male_file = os.path.join(self.base_dir, 'congratulations', 'male.txt')
        self.congrats_female_file = os.path.join(self.base_dir, 'congratulations', 'female.txt')
        
    def get_channel_id_by_name(self, channel_name):
        try:
            teams = self.driver.teams.get_teams()
            for team in teams:
                channels = self.driver.channels.get_channels_for_team(team['id'])
                for channel in channels:
                    if channel['name'] == channel_name:
                        return channel['id']
            return None
        except Exception as e:
            print(f"Ошибка получения ID канала: {e}")
            return None
            
    def load_congratulations_by_gender(self, gender):
        file_path = self.congrats_male_file if gender == 'male' else self.congrats_female_file
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                texts = f.readlines()
            return [t.strip() for t in texts if t.strip()]
        except Exception as e:
            print(f"Ошибка загрузки поздравлений для {gender}: {e}")
            default_texts = {
                'male': "С днём рождения, коллега!",
                'female': "С днём рождения, коллега!"
            }
            return [default_texts.get(gender, "С днём рождения!")]
            
    def get_random_image_by_gender(self, gender):
        images_dir = self.images_male_dir if gender == 'male' else self.images_female_dir
        
        if not os.path.exists(images_dir):
            print(f"Папка с картинками не найдена: {images_dir}")
            return None
            
        images = [f for f in os.listdir(images_dir) 
                 if f.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp'))]
        
        if not images:
            print(f"В папке {images_dir} нет картинок")
            return None
            
        return os.path.join(images_dir, random.choice(images))
        
    def get_gender_emoji(self, gender):
        return "👨" if gender == 'male' else "👩"
        
    def get_gender_title(self, gender):
        return "Уважаемый" if gender == 'male' else "Уважаемая"
        
    def send_birthday_message(self, username, full_name, gender):
        congratulations = self.load_congratulations_by_gender(gender)
        congrat_text = random.choice(congratulations)
        
        gender_emoji = self.get_gender_emoji(gender)
        gender_title = self.get_gender_title(gender)
        
        message = f"🎉 **Поздравляем с днём рождения!** 🎉\n\n"
        message += f"{gender_emoji} **{full_name}** (@{username})\n\n"
        message += f"{gender_title} {full_name}!\n\n"
        message += f"{congrat_text}\n\n"
        message += f"🎂 Желаем счастья, здоровья и успехов! 🎂"
        
        try:
            self.driver.posts.create_post({
                'channel_id': self.notification_channel_id,
                'message': message
            })
            print(f"Отправлено поздравление для {full_name} ({gender})")
        except Exception as e:
            print(f"Ошибка отправки сообщения: {e}")
        
        image_path = self.get_random_image_by_gender(gender)
        if image_path and os.path.exists(image_path):
            try:
                with open(image_path, 'rb') as f:
                    self.driver.files.upload_file(
                        channel_id=self.notification_channel_id,
                        files={'files': (os.path.basename(image_path), f, 'image/jpeg')}
                    )
                print(f"Отправлена картинка для {gender}: {os.path.basename(image_path)}")
            except Exception as e:
                print(f"Ошибка отправки картинки: {e}")
        else:
            print(f"Картинка не найдена для пола {gender}")
                
    def check_birthdays(self):
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Проверка дней рождения...")
        birthdays = self.db.get_today_birthdays()
        
        if not birthdays:
            print("Сегодня нет дней рождения")
            return
            
        print(f"Найдено именинников: {len(birthdays)}")
        for user in birthdays:
            print(f"Поздравляем {user['username']} (пол: {user['gender']})")
            self.send_birthday_message(
                user['username'], 
                user['full_name'], 
                user['gender']
            )
    
    def show_help(self):
        """Показывает справку по командам"""
        help_text = """
**🤖 Команды бота дней рождения**

**📝 Добавление пользователя:**