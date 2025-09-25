import platform
import requests
import telebot
from telebot.types import Message
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
if (platform.system()) == 'Windows':
    CONFIG_PATH = 'C:\\local_config\\my_global_config.cfg'
else:
    CONFIG_PATH = '/home/semen106/bot/my_global_config.cfg'

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

db_creds = config['HOSTER_KC_DB'] if platform.system() == 'Windows' else config['HOSTER_KC_DB_LOCAL']

TELEGRAM_TOKEN = config['REMINDBOT2']['remindbot_token']
BIRTHDAY_CHAT_WITH_NIKA = 'birthday_chat_with_nika'  # Замените на нужный chat_id

ADMIN_CHAT_ID = int(config['REMINDBOT2']['admin_chat_id'])  # id админа из конфига
CHAT_ID = int(config['REMINDBOT2']['birthday_chat_with_nika'])  # id основного чата из конфига

progroup_html_icon = f'<tg-emoji emoji-id="5249481442043393850">🦆</tg-emoji>'
bunker_html_icon = f'<tg-emoji emoji-id="5249462587136966291">🧟‍♂️</tg-emoji>'

bot = telebot.TeleBot(TELEGRAM_TOKEN)

# Приветственное сообщение админу при запуске
try:
    bot.send_message(ADMIN_CHAT_ID, 'Бот напоминалка успешно запущен!')
    logging.info('Приветственное сообщение админу отправлено.')
except Exception as e:
    logging.error(f'Ошибка при отправке приветственного сообщения админу: {e}')


def get_last_sync_date():
    try:
        conn = psycopg2.connect(**db_creds)
        cur = conn.cursor()
        cur.execute("""
            SELECT "current_timestamp" 
            FROM nsi_data.dict_portal_ac_employees_tb_form 
            ORDER BY "current_timestamp" DESC 
            LIMIT 1
        """)
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if result:
            return result[0].strftime('%d.%m.%Y %H:%M')
        return "Неизвестно"
    except Exception as e:
        logging.error(f'Ошибка при получении даты последней синхронизации: {e}')
        return "Неизвестно"

def get_next_5_birthdays(all_employees=False):
    try:
        logging.info('Подключение к базе данных для получения следующих 5 дней рождений...')
        conn = psycopg2.connect(**db_creds)
        cur = conn.cursor()
        if all_employees:
            # Получаем всех сотрудников, независимо от подразделения
            cur.execute("""
                --Следующие 5 дней рождений всех сотрудников
                SELECT DISTINCT ON (fullname) 
                substring(fullname from '^[^ ]+ [^ ]+') as fullname, birthday
                FROM nsi_data.dict_portal_ac_employees_tb_form
                where status is true and "current_timestamp" = (select "current_timestamp" cs from nsi_data.dict_portal_ac_employees_tb_form order by cs desc limit 1)
                ORDER BY fullname
            """)
        else:
            cur.execute("""
                --Следующие 5 дней рождений сотрудников
                SELECT DISTINCT ON (fullname) substring(fullname from '^[^ ]+ [^ ]+') as fullname, birthday
                FROM nsi_data.dict_portal_ac_employees_tb_form
                where status is true and "current_timestamp" = (select "current_timestamp" cs from nsi_data.dict_portal_ac_employees_tb_form order by cs desc limit 1)
                and department ilike any(array['%перационная%','%роект%','%мультимед%','%руковод%'])
                and birthday is not null
                ORDER BY fullname
            """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            logging.info('Нет данных о днях рождения.')
            return []

        today = datetime.now().date()
        birthday_data = []

        for fullname, birthday in rows:
            try:
                day, month = map(int, birthday.split('.'))
                # Рассчитываем дату в текущем году
                bday_this_year = datetime(today.year, month, day).date()
                # Если день рождения уже прошел, берем следующий год
                if bday_this_year < today:
                    bday_next_year = datetime(today.year + 1, month, day).date()
                    days_until = (bday_next_year - today).days
                else:
                    days_until = (bday_this_year - today).days

                birthday_data.append((fullname, birthday, days_until))
            except Exception as e:
                log_info(f"Ошибка преобразования даты для {fullname}: {birthday} ({e})")
                continue

        # Сортируем по количеству дней до дня рождения
        birthday_data.sort(key=lambda x: x[2])

        # Берем первые 5 уникальных дат
        result = []
        unique_days = set()

        for fullname, birthday, days_until in birthday_data:
            if len(unique_days) < 5 or days_until in unique_days:
                result.append((fullname, birthday, days_until))
                unique_days.add(days_until)
            elif len(unique_days) >= 5:
                break

        logging.info(f'Получено {len(result)} записей следующих дней рождений.')
        return result

    except Exception as e:
        logging.error(f'Ошибка при получении следующих дней рождений: {e}')
        return []

def get_vacations(all_employees=False):
    """Получение данных об отпусках сотрудников из базы данных"""
    try:
        logging.info('Подключение к базе данных для получения данных об отпусках...')
        conn = psycopg2.connect(**db_creds)
        cur = conn.cursor()
        
        if all_employees:
            # Получаем отпуска всех сотрудников
            cur.execute("""
                SELECT DISTINCT ON (fullname,vac_date_start) substring(fullname from '^[^ ]+ [^ ]+') as fullname, vac_date_start, vac_date_end
                FROM nsi_data.dict_portal_ac_employees_tb_form
                WHERE status is true 
                AND "current_timestamp" = (SELECT "current_timestamp" cs FROM nsi_data.dict_portal_ac_employees_tb_form ORDER BY cs DESC LIMIT 1)
                AND vac_date_start IS NOT NULL
                AND vac_date_end IS NOT NULL
                ORDER BY fullname
            """)
        else:
            # Получаем отпуска сотрудников определенных подразделений
            cur.execute("""
                SELECT DISTINCT ON (fullname,vac_date_start) substring(fullname from '^[^ ]+ [^ ]+') as fullname, vac_date_start, vac_date_end
                FROM nsi_data.dict_portal_ac_employees_tb_form
                WHERE status is true 
                AND "current_timestamp" = (SELECT "current_timestamp" cs FROM nsi_data.dict_portal_ac_employees_tb_form ORDER BY cs DESC LIMIT 1)
                AND department ILIKE ANY(ARRAY['%перационная%','%роект%','%мультимед%','%руковод%'])
                AND vac_date_start IS NOT NULL
                AND vac_date_end IS NOT NULL
                ORDER BY fullname
            """)
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        logging.info(f'Получено {len(rows)} записей об отпусках.')
        return rows
    except Exception as e:
        logging.error(f'Ошибка при получении данных об отпусках: {e}')
        return []

def get_current_and_upcoming_vacations(all_employees=False):
    """Получение текущих и предстоящих отпусков"""
    try:
        vacations = get_vacations(all_employees=all_employees)
        if not vacations:
            return []
        
        today = datetime.now().date()
        vacation_data = []
        
        for fullname, vacation_start, vacation_end in vacations:
            try:
                # Преобразуем строки дат в объекты date
                if isinstance(vacation_start, str):
                    start_date = datetime.strptime(vacation_start, '%d.%m.%Y').date()
                else:
                    start_date = vacation_start
                    
                if isinstance(vacation_end, str):
                    end_date = datetime.strptime(vacation_end, '%d.%m.%Y').date()
                else:
                    end_date = vacation_end
                
                # Проверяем, актуален ли отпуск (текущий или предстоящий в течение 30 дней)
                days_until_start = (start_date - today).days
                days_until_end = (end_date - today).days
                
                if days_until_end >= 0 and days_until_start <= 30:  # Отпуск актуален
                    vacation_data.append((fullname, start_date, end_date, days_until_start))
                    
            except Exception as e:
                log_info(f"Ошибка преобразования дат отпуска для {fullname}: {e}")
                continue
        
        # Сортируем по дате начала отпуска
        vacation_data.sort(key=lambda x: x[1])
        
        logging.info(f'Получено {len(vacation_data)} актуальных отпусков.')
        return vacation_data
        
    except Exception as e:
        logging.error(f'Ошибка при обработке данных об отпусках: {e}')
        return []

def send_vacation_notifications(chat_id, all_employees=False):
    """Отправка уведомлений об отпусках в чат"""
    try:
        vacations = get_current_and_upcoming_vacations(all_employees=all_employees)
        if not vacations:
            bot.send_message(chat_id, "Нет данных о текущих и предстоящих отпусках.")
            return

        if not all_employees:
            message = f"{progroup_html_icon} <b>ОТПУСКА PRO</b>ГРУППЫ:\n\n"
        else:
            message = f"{bunker_html_icon} ОТПУСКА БУНКЕРА:\n\n"

        # Группируем отпуска по категориям
        current_vacations = []
        starting_soon = []
        upcoming_vacations = []

        today = datetime.now().date()

        for fullname, start_date, end_date, days_until_start in vacations:
            if days_until_start <= 0 and (end_date - today).days >= 0:
                # Текущий отпуск
                days_left = (end_date - today).days + 1
                current_vacations.append((fullname, start_date, end_date, days_left))
            elif days_until_start <= 3:
                # Начинается в ближайшие 3 дня
                starting_soon.append((fullname, start_date, end_date, days_until_start))
            else:
                # Предстоящие отпуска
                upcoming_vacations.append((fullname, start_date, end_date, days_until_start))

        # Формируем сообщение
        if current_vacations:
            message += "🏖️ <b>В отпуске сейчас:</b>\n"
            # Сортируем по остатку дней от меньшего к большему
            current_vacations.sort(key=lambda x: x[3])  # x[3] - это days_left
            for fullname, start_date, end_date, days_left in current_vacations:
                # Извлекаем фамилию и инициалы
                name_parts = fullname.split()
                if len(name_parts) >= 3:
                    surname = name_parts[0]
                    first_initial = name_parts[1][0] if name_parts[1] else ""
                    second_initial = name_parts[2][0] if name_parts[2] else ""
                    formatted_name = f"{surname} {first_initial}.{second_initial}."
                else:
                    formatted_name = fullname
                
                if days_left > 0:
                    message += f"  {start_date.strftime('%d.%m')}-{end_date.strftime('%d.%m')}, ост: {days_left} дн., {formatted_name}\n"
                else:
                    message += f"  {start_date.strftime('%d.%m')}-{end_date.strftime('%d.%m')}, ост: последний день, {formatted_name}\n"
            message += "\n"

        if starting_soon:
            message += "🎒 <b>Уходят в отпуск скоро:</b>\n"
            for fullname, start_date, end_date, days_until_start in starting_soon:
                message += f"  {fullname}\n"
                message += f"     📅 {start_date.strftime('%d.%m')} - {end_date.strftime('%d.%m')}\n"
                if days_until_start == 0:
                    message += f"     🚀 начинается сегодня\n"
                elif days_until_start == 1:
                    message += f"     🚀 начинается завтра\n"
                else:
                    message += f"     🚀 через {days_until_start} дн.\n"
            message += "\n"

        if upcoming_vacations[:5]:  # Показываем только первые 5
            message += "📋 <b>Планируемые отпуска:</b>\n"
            for fullname, start_date, end_date, days_until_start in upcoming_vacations[:5]:
                # Извлекаем фамилию и инициалы
                name_parts = fullname.split()
                if len(name_parts) >= 3:
                    surname = name_parts[0]
                    first_initial = name_parts[1][0] if name_parts[1] else ""
                    second_initial = name_parts[2][0] if name_parts[2] else ""
                    formatted_name = f"{surname} {first_initial}.{second_initial}."
                else:
                    formatted_name = fullname
        
                message += f"  {start_date.strftime('%d.%m')}-{end_date.strftime('%d.%m')}, через: {days_until_start} дн., {formatted_name}\n"
            if len(upcoming_vacations) > 5:
                message += f"     ... и еще {len(upcoming_vacations) - 5} отпусков\n"
            message += "\n"

        last_sync = get_last_sync_date()
        message += f"📊 Данные актуальны на: {last_sync}"

        bot.send_message(chat_id, message, parse_mode='html')
        logging.info(f'Уведомления об отпусках отправлены в чат {chat_id}.')

    except Exception as e:
        logging.error(f'Ошибка при отправке уведомлений об отпусках: {e}')
        bot.send_message(chat_id, "Произошла ошибка при получении данных об отпусках.")

def send_next_5_birthdays(chat_id, all_employees=False):
    try:
        birthdays = get_next_5_birthdays(all_employees=all_employees)
        if not birthdays:
            bot.send_message(chat_id, "Нет данных о ближайших днях рождения.")
            return

        if not all_employees:
            message = f"{progroup_html_icon} <b>PRO</b>ГРУППА:\n\n"
        else:
            message = f"{bunker_html_icon} БУНКЕР:\n\n"
        current_days = None

        # Группируем дни рождения по категориям
        today_birthdays = []
        tomorrow_birthdays = []
        later_birthdays = []
        
        for fullname, birthday, days_until in birthdays:
            if days_until == 0:
                today_birthdays.append((fullname, birthday))
            elif days_until == 1:
                tomorrow_birthdays.append((fullname, birthday))
            else:
                later_birthdays.append((fullname, birthday, days_until))
        
        # Формируем сообщение по блокам
        if today_birthdays:

            message += "🎉 Сегодня:\n"
            for fullname, birthday in today_birthdays:
                message += f" {fullname} ({birthday})\n"
            message += "\n"
        
        if tomorrow_birthdays:
            message += "🎈 Завтра:\n"
            for fullname, birthday in tomorrow_birthdays:
                message += f" {fullname} ({birthday})\n"
            message += "\n"
        
        if later_birthdays:
            message += "📅 Уже скоро:\n"
            for fullname, birthday, days_until in later_birthdays:
                message += f"  {fullname} ({birthday})  через {days_until} дней\n"
            message += "\n"

        last_sync = get_last_sync_date()
        message += f"📊 Данные актуальны на: {last_sync}"
        bot.send_message(chat_id, message, parse_mode='html')
        logging.info(f'Список следующих 5 дней рождений отправлен в чат {chat_id}.')

    except Exception as e:
        logging.error(f'Ошибка при отправке списка следующих дней рождений: {e}')
        print(e)
        bot.send_message(chat_id, "Произошла ошибка при получении данных о днях рождения.")


def get_birthdays(all_employees=False):
    try:
        logging.info('Подключение к базе данных...')
        conn = psycopg2.connect(
            **db_creds
            # host=DB_HOST,
            # port=DB_PORT,
            # dbname=DB_NAME,
            # user=DB_USER,
            # password=DB_PASSWORD,
            # connect_timeout=CONNECT_TIMEOUT
        )
        cur = conn.cursor()
        if all_employees:
            # Получаем всех сотрудников, независимо от статуса
            cur.execute("""
                --Дни рождения всех сотрудников
                SELECT DISTINCT ON (fullname) substring(fullname from '^[^ ]+ [^ ]+') as fullname, birthday
                FROM nsi_data.dict_portal_ac_employees_tb_form
                where status is true and "current_timestamp" = (select "current_timestamp" cs from nsi_data.dict_portal_ac_employees_tb_form order by cs desc limit 1)
                ORDER BY fullname
            """)
        else:
            cur.execute("""
                --Дни рождения сотрудников
                SELECT DISTINCT ON (fullname) substring(fullname from '^[^ ]+ [^ ]+') as fullname, birthday
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
        wrapped_fullname = wrap_text(fullname, width=50)
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
        # Увеличиваем ширину фигуры, чтобы ФИО не переносилось и не вылетало за пределы ячейки
        fig_width = 14  # увеличено с 10 до 14
        fig_height = 0.5 + 0.7 * len(df)
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))
        ax.axis('off')
        tbl = ax.table(cellText=df.values, colLabels=df.columns, loc='center', cellLoc='center')
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(16)
        tbl.scale(2.5, 2)  # увеличиваем ширину ячеек
        # Дополнительно увеличим ширину столбца "ФИО"
        for (row, col), cell in tbl.get_celld().items():
            if col == 1:  # "ФИО" обычно второй столбец (0 - Категория, 1 - ФИО, 2 - Дата рождения)
                cell.set_width(0.45)
            else:
                cell.set_width(0.25)
            cell.set_height(0.15)
            cell.set_fontsize(16)
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', dpi=300)
        buf.seek(0)
        plt.close(fig)
        bot.send_photo(chat_id, buf)
        logging.info(f'Напоминание успешно отправлено в чат {chat_id}.')
    except Exception as e:
        logging.error(f'Ошибка при отправке напоминания: {e}')

# Обработка команды /birthdays и любого текстового сообщения
# Обработка команды /birthdays и /next5
@bot.message_handler(commands=['birthdays'])
def handle_birthdays_command(message):
    send_birthday_reminder(chat_id=message.chat.id) 


@bot.message_handler(commands=['next5'])
def handle_next5_command(message: Message):
    if message.from_user.id == ADMIN_CHAT_ID:
        send_next_5_birthdays(chat_id=message.chat.id)
    else:
        bot.send_message(message.chat.id, "У вас нет прав для выполнения этой команды.")

@bot.message_handler(commands=['next5all'])
def handle_next5all_command(message: Message):
    if message.from_user.id == ADMIN_CHAT_ID:
        send_next_5_birthdays(chat_id=message.chat.id, all_employees=True)
    else:
        bot.send_message(message.chat.id, "У вас нет прав для выполнения этой команды.")
        
@bot.message_handler(commands=['vacations'])
def handle_vacations_command(message: Message):
    if message.from_user.id == ADMIN_CHAT_ID:
        send_vacation_notifications(chat_id=message.chat.id)
    else:
        bot.send_message(message.chat.id, "У вас нет прав для выполнения этой команды.")

@bot.message_handler(commands=['vacationsall'])
def handle_vacationsall_command(message: Message):
    if message.from_user.id == ADMIN_CHAT_ID:
        send_vacation_notifications(chat_id=message.chat.id, all_employees=True)
    else:
        bot.send_message(message.chat.id, "У вас нет прав для выполнения этой команды.")

def scheduler():
    times = ["03:30"]
    while True:
        logging.info('Запуск планировщика...')
        # Получаем текущее время и определяем ближайший target
        now = datetime.now()
        logging.info(f"Текущее время: {now}")
        # Список будущих target на сегодня
        future_targets = [
            # Создаем datetime объект для каждого target времени
            # например, если сейчас 14:00, то target будет 15:30
            now.replace(hour=int(t[:2]), minute=int(t[3:]), second=0, microsecond=0)
            for t in times
            if now.replace(hour=int(t[:2]), minute=int(t[3:]), second=0, microsecond=0) > now
        ]
        if future_targets:
            target = min(future_targets)
        else:
            # Все target на сегодня прошли, берем самое раннее на завтра
            t_earliest = times[0]
            target = (now + timedelta(days=1)).replace(hour=int(t_earliest[:2]), minute=int(t_earliest[3:]), second=0, microsecond=0)
        wait_seconds = (target - now).total_seconds()
        logging.info(f'Ожидание до следующей отправки: {wait_seconds/60:.1f} минут.')
        time.sleep(wait_seconds)
        # send_birthday_reminder()
        send_next_5_birthdays(CHAT_ID)
        send_next_5_birthdays(CHAT_ID, all_employees=True)
        send_vacation_notifications(CHAT_ID)


# Основной цикл запуска бота
def run_bot():
    # Запускаем scheduler только один раз
    thread = threading.Thread(target=scheduler, daemon=True)
    thread.start()
    while True:
        try:
            logging.info('Бот запущен.')
            bot.polling(none_stop=True, interval=0)
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка сети: {e}. Перезапуск через 15 секунд.")
            print(f"Ошибка сети: {e}. Перезапуск через 15 секунд.")
            time.sleep(15)
        except Exception as e:
            logging.error(f"Произошла непредвиденная ошибка: {e}. Перезапуск через 30 секунд.")
            print(f"Произошла непредвиденная ошибка: {e}. Перезапуск через 30 секунд.")
            time.sleep(30)


if __name__ == '__main__':
    run_bot()