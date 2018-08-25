# Operations pertaining to AWS services
import botocore
import boto3
from logging import getLogger

logger = getLogger(__name__)


def glacier_upload(backup_path, vault):
    """ Upload db dump to Amazon Glaier
    Args:
        backup_file: Path to backup file
        vault: Vault to upload to
    Returns:
        Returns response from AWS
    """
    # Create boto aws client
    client = boto3.client('glacier')

    # Open db dump
    try:
        with open(backup_path, 'rb') as dump:
            # Upload dump
            try:
                response = client.upload_archive(vaultName=vault, body=dump)
            except botocore.exceptions.NoCredentialsError:
                logger.error('Credentials Not Found')
                raise
            except client.exceptions.ResourceNotFoundException:
                logger.error('Vault not found')
                raise
            except botocore.exceptions.ClientError as e:
                logger.error(e)
                raise
    except OSError:
        logger.error('Failed to open db dump %s for read', backup_path)
        raise

    logger.info('Uploaded %s to AWS Glacier', backup_path)

    return response
