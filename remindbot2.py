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

def wrap_text(text, width=20):
    # Перенос строки каждые width символов
    return '\n'.join([text[i:i+width] for i in range(0, len(text), width)])

def format_birthday_dataframe():
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)
    # Границы следующей недели
    # Пример работы этих строк:
    # today = datetime(2024, 6, 7).date()  # допустим, сегодня пятница (weekday() == 4)
    # days_until_next_monday = (7 - today.weekday()) % 7 or 7
    # days_until_next_monday = (7 - 4) % 7 or 7 = 3
    # next_monday = today + timedelta(days=3)  # будет понедельник, 10 июня 2024
    # next_sunday = next_monday + timedelta(days=6)  # будет воскресенье, 16 июня 2024

    days_until_next_monday = (7 - today.weekday()) % 7 or 7
    next_monday = today + timedelta(days=days_until_next_monday)
    next_sunday = next_monday + timedelta(days=6)
    next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)

    birthdays = get_birthdays()
    data = []


    # logging.info(f"Сегодня: {today}")
    # logging.info(f"Завтра: {tomorrow}")
    # logging.info(f"След. неделя: {next_sunday}")
    # logging.info(f"След. месяц: {next_month}")

    for fullname, birthday in birthdays:
        # logging.info(f"Обработка: {fullname} {birthday}")
        if not birthday:
            continue
        # birthday: строка 'DD.MM'
        try:
            day, month = map(int, birthday.split('.'))
            bday = datetime(today.year, month, day).date()
        except Exception as e:
            log_info(f"Ошибка преобразования даты для {fullname}: {birthday} ({e})")
            continue

        # logging.info(f"Дата рождения: {bday}")   
        wrapped_fullname = wrap_text(fullname, width=20)
        if bday == today:
            # logging.info(f"Сегодня: {fullname} {birthday}")
            data.append([0, "Сегодня", wrapped_fullname, f"{birthday}"])
        elif bday == tomorrow:
            # logging.info(f"Завтра: {fullname} {birthday}")
            data.append([1, "Завтра", wrapped_fullname, f"{birthday}"])
        elif next_monday <= bday <= next_sunday:
            data.append([2, "На след. неделе", wrapped_fullname, f"{birthday}"])
        elif bday.month == next_month.month:
            # logging.info(f"В след. месяце: {fullname} {birthday}")
            data.append([3, "В след. месяце", wrapped_fullname, f"{birthday}"])

    if not data:
        data.append([4, "-", "Нет ближайших дней рождений", "-"])

    df = pd.DataFrame(data, columns=["sort", "Категория", "ФИО", "Дата рождения"])
    df = df.sort_values(by="sort").drop(columns=["sort"])
    logging.info(f"Данные для отправки: {df}")
    return df

def send_birthday_reminder(chat_id=CHAT_ID):
    try:
        df = format_birthday_dataframe()
        # Увеличиваем ширину фигуры и ячеек, чтобы ФИО не переносилось на вторую строку
        fig_width = 18  # увеличено для предотвращения переноса ФИО
        fig_height = 0.5 + 0.7 * len(df)
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))
        ax.axis('off')
        tbl = ax.table(cellText=df.values, colLabels=df.columns, loc='center', cellLoc='center')
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(16)
        tbl.scale(3.5, 2)  # увеличиваем ширину ячеек ещё больше

        # Увеличиваем ширину столбца "ФИО" для предотвращения переноса
        for (row, col), cell in tbl.get_celld().items():
            if col == 1:  # "ФИО" обычно второй столбец (0 - Категория, 1 - ФИО, 2 - Дата рождения)
                cell.set_width(0.65)  # увеличено для ФИО
            else:
                cell.set_width(0.25)
            cell.set_height(0.15)
            cell.set_fontsize(16)
            # Отключаем автоматический перенос текста (если вдруг wrap_text где-то применялся)
            cell.get_text().set_wrap(False)
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', dpi=300)
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
                 "16:30", "17:00", "17:30", "18:00", "18:30", "19:00", "19:30"]
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