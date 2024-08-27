import asyncio
import logging
import random
import string
from datetime import datetime

import aiosqlite

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

DB_PATH = 'messages.db'


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            '''
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content TEXT NOT NULL,
                    timestamp TEXT NOT NULL
                )
            '''
        )
        await db.commit()


async def handle_client(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
):
    """Обработка сообщения от клиента."""
    addr = writer.get_extra_info('peername')
    logger.info(f'Новое соединение от {addr}')
    async with aiosqlite.connect(DB_PATH) as db:
        while True:
            data = await reader.readline()
            if not data:
                logger.info(f'Соединение закрыто {addr}')
                writer.close()
                await writer.wait_closed()
                break

            message = data.decode().rstrip()
            timestamp = datetime.now().isoformat()
            logger.info(f'Получено сообщение от {addr}: {message}')

            await db.execute(
                'INSERT INTO messages (content, timestamp) VALUES (?, ?)',
                (message, timestamp)
            )
            await db.commit()

            writer.write(data)
            await writer.drain()


async def start_server(host='127.0.0.1', port=8888):
    """Запуск сервера."""
    server = await asyncio.start_server(handle_client, host, port)
    addr = server.sockets[0].getsockname()
    logger.info(f'Server запущен на {addr}')
    async with server:
        await server.serve_forever()


async def tcp_client(client_id, host='127.0.0.1', port=8888):
    """Отправка и чтение сообщений клиентом."""
    reader, writer = await asyncio.open_connection(host, port)
    logger.info(f'Клиент {client_id} подключен к серверу')

    for i in range(5):
        await asyncio.sleep(random.randint(5, 10))
        message = f'Сообщение от клиента {client_id}: ' + ''.join(
            random.choices(string.ascii_letters + string.digits, k=20)
        )
        writer.write((message + '\n').encode())
        await writer.drain()
        logger.info(f'Клиент {client_id} отправил: {message}')

        data = await reader.readline()
        echo_message = data.decode().rstrip()
        logger.info(f'Клиент {client_id} получил эхо: {echo_message}')

    logger.info(f'Клиент {client_id} закрывает соединение')
    writer.close()
    await writer.wait_closed()


async def start_clients():
    """Запуск клиентских задач для взаимодействия с сервером."""
    tasks = [
        asyncio.create_task(tcp_client(client_id))
        for client_id in range(1, 11)
    ]
    await asyncio.gather(*tasks)
    logger.info('Все клиенты завершили работу')


async def main():
    await init_db()
    server_task = asyncio.create_task(start_server())
    # Ждем запуска сервера перед запуском клиентов
    await asyncio.sleep(1)
    clients_task = asyncio.create_task(start_clients())

    await clients_task  # Ждем завершения всех клиентов

    # После завершения клиентов можем остановить сервер, если необходимо
    logger.info('Все клиентские задачи завершены. Программа завершает работу.')
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        logger.info('Серверная задача была остановлена.')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Программа была прервана пользователем.')
