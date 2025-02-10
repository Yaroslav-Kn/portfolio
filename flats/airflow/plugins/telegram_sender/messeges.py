from airflow.providers.telegram.hooks.telegram import TelegramHook
from dotenv import load_dotenv
import os

def __send_message_telegram(message: str) -> None:
    # получаем переменные
    load_dotenv()
    TOKEN = os.environ.get("TEL_TOKEN")
    CHAT_ID = os.environ.get("TEL_CHAT_ID")
    hook = TelegramHook(telegram_conn_id='telegram',
                        token=TOKEN,
                        chat_id=CHAT_ID)  

    # отправляем сообщение 
    hook.send_message({
        'chat_id': CHAT_ID,
        'text': message
    })

def send_telegram_success_message(context: dict[str, str]) -> None:
    dag = context['dag'].dag_id
    run_id = context['run_id']    
    CHECK_MARK = u'\U00002705'
    message = f'{CHECK_MARK} Исполнение DAG {dag} с id={run_id} прошло успешно!'
    __send_message_telegram(message)


def send_telegram_failure_message(context: dict[str, str]) -> None:
    dag = context['dag'].dag_id
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    CROSS_MARK = u'\U0000274C'
    message = f'{CROSS_MARK} DAG {dag} с id={run_id} завершён с ошибкой! {task_instance_key_str}'
    __send_message_telegram(message)