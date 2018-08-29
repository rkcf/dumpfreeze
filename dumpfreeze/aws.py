# Operations pertaining to AWS services
import botocore
import boto3
from logging import getLogger

logger = getLogger(__name__)


def glacier_upload(backup_path, vault):
    """ Upload db dump to Amazon Glaier
    Args:
        backup_path: Path to backup file
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


def retrieve_archive(archive_info):
    """ Initates an archive retrieval job
    Args:
        archive_info: inventorydb.Archive object
    Returns:
        Returns job metadata
    """
    # Create boto archive object
    glacier = boto3.resource('glacier')
    archive = glacier.Archive(archive_info.location.split('/')[1],
                              archive_info.vault_name,
                              archive_info.aws_id)

    # Send request to initiate retrieval job
    response = archive.initiate_archive_retrieval()

    logger.info('initated archive retrieval of %s', archive)

    return((response.account_id, response.vault_name, response.id))


def delete_archive(archive_info):
    """ Delete an archive
    Args:
        archive_info: inventorydb.Archive object
    """
    # Create boto archive object
    glacier = boto3.resource('glacier')
    archive = glacier.Archive(archive_info.location.split('/')[1],
                              archive_info.vault_name,
                              archive_info.aws_id)
    archive.delete()
    logger.info('Deleted Archive %s', archive_info.id)


def check_job(job_info):
    """ Check if job is complete
    Args:
        job_info: inventorydb.Job object
    Returns:
        Returns True if job is complete
    """
    # Create boto job object
    glacier = boto3.resource('glacier')
    job = glacier.Job(job_info.account_id, job_info.vault_name, job_info.id)

    # Reload job info
    job.load()

    return job.completed


def get_archive_data(job_info):
    """ Retrieves archive data
    Args:
        job_info: inventorydb.Job object
    Returns:
        Returns the archive body
    """
    # Create boto job object
    glacier = boto3.resource('glacier')
    job = glacier.Job(job_info.account_id, job_info.vault_name, job_info.id)

    # Get output
    output = job.get_output()

    # Convert output from StreamingBody
    body = output['body'].read().decode('utf-8')

    return body


def get_job_archive(job_info):
    """ Get corrosponding AWS archive id of job
    Args:
        job_info: inventorydb.Job object
    Returns:
        Returns the AWS archive id
    """
    glacier = boto3.resource('glacier')
    job = glacier.Job(job_info.account_id, job_info.vault_name, job_info.id)

    return job.archive_iD
