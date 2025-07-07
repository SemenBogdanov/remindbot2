import telebot
import psycopg2
from datetime import datetime, timedelta
import time
import threading
import pandas as pd
import matplotlib.pyplot as plt
import io
import configparser
import logging
import os
import inspect

# Настройка логгирования
LOG_PATH = os.path.join(os.path.dirname(__file__), 'bot.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def log_info(msg):
    frame = inspect.currentframe().f_back
    logging.info(f"[{frame.f_code.co_name}:{frame.f_lineno}] {msg}")

# Чтение конфига
CONFIG_PATH = '/home/semen106/bot/my_global_config.cfg'
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

DB_HOST = config['HOSTER_KC_DB_LOCAL']['host']
DB_PORT = int(config['HOSTER_KC_DB_LOCAL']['port'])
DB_NAME = config['HOSTER_KC_DB_LOCAL']['database']
DB_USER = config['HOSTER_KC_DB_LOCAL']['user']
DB_PASSWORD = config['HOSTER_KC_DB_LOCAL']['password']

TELEGRAM_TOKEN = config['REMINDBOT2']['remindbot_token']
BIRTHDAY_CHAT_WITH_NIKA = 'birthday_chat_with_nika'  # Замените на нужный chat_id

ADMIN_CHAT_ID = int(config['REMINDBOT2']['admin_chat_id'])  # id админа из конфига
CHAT_ID = int(config['REMINDBOT2']['birthday_chat_with_nika'])  # id основного чата из конфига

bot = telebot.TeleBot(TELEGRAM_TOKEN)

# Приветственное сообщение админу при запуске
try:
    bot.send_message(ADMIN_CHAT_ID, 'Бот напоминалка успешно запущен!')
    logging.info('Приветственное сообщение админу отправлено.')
except Exception as e:
    logging.error(f'Ошибка при отправке приветственного сообщения админу: {e}')

def get_birthdays():
    try:
        logging.info('Подключение к базе данных...')
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            # connect_timeout=CONNECT_TIMEOUT
        )
        cur = conn.cursor()
        cur.execute("""
            --Дни рождения сотрудников
            SELECT DISTINCT ON (fullname) fullname, birthday
            FROM nsi_data.dict_portal_ac_employees_tb_form
            where status is true and "current_timestamp" = (select "current_timestamp" cs from nsi_data.dict_portal_ac_employees_tb_form order by cs desc limit 1)
            and department ilike any(array['%перационная%','%роект%','%мультимед%','%руковод%'])
            ORDER BY fullname
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        logging.info(f'Получено {len(rows)} записей из базы.')
        return rows
    except Exception as e:
        logging.error(f'Ошибка при работе с базой данных: {e}')
        return []

def format_birthday_dataframe():
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)
    next_week = today + timedelta(days=7)
    next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)

    birthdays = get_birthdays()
    data = []

    for fullname, birthday in birthdays:
        if not birthday:
            continue
        # birthday: строка 'DD.MM'
        try:
            day, month = map(int, birthday.split('.'))
            bday = datetime(today.year, month, day).date()
        except Exception as e:
            log_info(f"Ошибка преобразования даты для {fullname}: {birthday} ({e})")
            continue
        if bday == today:
            logging.info(f"Сегодня: {fullname} {birthday}")
            data.append(["Сегодня", fullname, f"{birthday}"])
        elif bday == tomorrow:
            logging.info(f"Завтра: {fullname} {birthday}")
            data.append(["Завтра", fullname, f"{birthday}"])
        elif today < bday <= next_week:
            logging.info(f"На след. неделе: {fullname} {birthday}")
            data.append(["На след. неделе", fullname, f"{birthday}"])
        elif bday.month == next_month.month:
            logging.info(f"В след. месяце: {fullname} {birthday}")
            data.append(["В след. месяце", fullname, f"{birthday}"])

    if not data:
        data.append(["-", "Нет ближайших дней рождений", "-"])

    df = pd.DataFrame(data, columns=["Категория", "ФИО", "Дата рождения"])
    logging.info(f"Данные для отправки: {df}")
    return df

def send_birthday_reminder(chat_id=CHAT_ID):
    try:
        df = format_birthday_dataframe()
        fig, ax = plt.subplots(figsize=(8, 0.5 + 0.5*len(df)))
        ax.axis('off')
        tbl = ax.table(cellText=df.values, colLabels=df.columns, loc='center', cellLoc='center')
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(12)
        tbl.scale(1, 1.5)
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', dpi=200)
        buf.seek(0)
        plt.close(fig)
        bot.send_photo(chat_id, buf)
        logging.info(f'Напоминание успешно отправлено в чат {chat_id}.')
    except Exception as e:
        logging.error(f'Ошибка при отправке напоминания: {e}')

# Обработка команды /birthdays и любого текстового сообщения
@bot.message_handler(commands=['birthdays'])
def handle_birthdays_command(message):
    send_birthday_reminder(chat_id=message.chat.id) 

@bot.message_handler(func=lambda message: True)
def handle_any_message(message):
    if message.chat.id == ADMIN_CHAT_ID:
        send_birthday_reminder(chat_id=message.chat.id)

def scheduler():
    while True:
        now = datetime.now()
        times = ["06:30", "13:30", "14:00", "14:30", "15:00", "15:30", "16:00", 
                 "16:30", "17:00", "17:30", "18:00", "18:30", "19:00"]
        for t in times:
            target = now.replace(hour=int(t[:2]), minute=int(t[3:]), second=0, microsecond=0)
            if now > target:
                target += timedelta(days=1)
            wait_seconds = (target - now).total_seconds()
            logging.info(f'Ожидание до следующей отправки: {wait_seconds/60:.1f} минут.')
            time.sleep(wait_seconds)
            send_birthday_reminder()

if __name__ == "__main__":
    logging.info('Бот запущен.')
    thread = threading.Thread(target=scheduler)
    thread.start()
    bot.polling(none_stop=True) 