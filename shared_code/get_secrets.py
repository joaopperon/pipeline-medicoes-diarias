import os
import logging

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


def get_secret(secret_name):
    key_vault_uri = os.getenv("KEY_VAULT_URI")

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_uri, credential=credential)

    try:
        secret = client.get_secret(secret_name)
        return secret.value
    except Exception as e:
        logging.error(f"Erro ao acessar segredo {secret_name}: {str(e)}")
        raise
