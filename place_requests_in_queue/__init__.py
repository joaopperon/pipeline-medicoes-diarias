import os
import pytz
import logging
import psycopg2
import traceback
import azure.functions as func

from datetime import datetime
from psycopg2.extras import DictCursor
from azure.storage.queue import QueueClient

from shared_code.get_secrets import get_secret


def fetch_pontos_medicao():
    logging.info("Buscando segredos em key-vault.")
    db_host = get_secret("DB-HOST")
    db_name = get_secret("DB-NAME")
    db_user = get_secret("DB-USER")
    db_port = get_secret("DB-PORT")
    db_password = get_secret("DB-USER-PASSWORD")

    logging.info("Estabelecendo conexão com banco de dados.")
    pg_connection = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password,
        port=db_port,
        cursor_factory=DictCursor,
    )

    query = """
        SELECT
	        uc.id AS id_unidade_consumidora,
	        uc.ccee_unidade_ludfor,
	        pm.id AS id_ponto_medicao, 
	        pm.codigo AS codigo_ponto_medicao
        FROM ponto_medicao pm 
        JOIN unidades_consumidoras uc
        ON uc.id_ponto_medicao = pm.id
        WHERE uc.valid_to IS NULL
        AND pm.valid_to IS null
    """

    with pg_connection:
        with pg_connection.cursor() as cursor:
            cursor.execute(query)
            response = cursor.fetchall()

            logging.info(
                f"Encontrados {len(response)} pontos medição para processamento."
            )
    return response


def place_in_queue(pontos_medicao: list):
    logging.info("Iniciando processo de envio de mensagens.")

    logging.info("Buscando variáveis de ambiente.")
    queue_name = os.getenv("QUEUE_NAME")
    queue_connection_string = os.getenv("AzureWebJobsStorage")

    logging.info("Estabelecendo client para fila.")
    queue_client = QueueClient.from_connection_string(
        conn_str=queue_connection_string, queue_name=queue_name
    )

    logging.info(f"Inserindo pedidos de requisição em fila {queue_name}.")
    for ponto_medicao in pontos_medicao:
        message = {
            **dict(ponto_medicao),
            "timestamp": datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
        }
        queue_client.send_message(content=message, time_to_live=12 * 3600)


def main(timer: func.TimerRequest):
    try:
        global TIMEZONE
        TIMEZONE = pytz.timezone("America/Sao_Paulo")

        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        logging.info(f"Iniciando criação de fila de requisições para o dia {today}.")

        pontos_medicao = fetch_pontos_medicao()
        place_in_queue(pontos_medicao)

        logging.info(f"Criação de fila de requisições para o dia {today} concluída.")

    except psycopg2.Error as db_error:
        logging.error("Erro de banco de dados:")
        logging.error(f"Tipo do erro: {type(db_error).__name__}")
        logging.error(f"Detalhes do erro: {str(db_error)}")
        logging.error(f"Traceback completo: {traceback.format_exc()}")
        raise

    except Exception as e:
        logging.error("Erro inesperado durante a execução:")
        logging.error(f"Tipo do erro: {type(e).__name__}")
        logging.error(f"Detalhes do erro: {str(e)}")
        logging.error(f"Traceback completo: {traceback.format_exc()}")
        raise
