import subprocess
import tempfile
from typing import Optional

import requests
import snakemake.common.tests
from snakemake_executor_plugin_google_lifesciences import ExecutorSettings
from snakemake_interface_executor_plugins import ExecutorSettingsBase
from google.cloud import storage


BUCKET_NAME = "snakemake-testing-%s-bucket" % next(tempfile._get_candidate_names())


class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsBase):
    __test__ = True

    def get_executor(self) -> str:
        return "google-lifesciences"
    
    def cleanup_test(self):
        cleanup_google_storage()

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        # instatiate ExecutorSettings of this plugin as appropriate
        return ExecutorSettings(
            keep_source_cache=False,
            service_account_email=get_default_service_account_email(),
        )

    def get_default_storage_provider(self) -> Optional[str]:
        # Return name of default remote provider if required for testing,
        # otherwise None.
        return "GS"

    def get_default_storage_prefix(self) -> Optional[str]:
        # Return default remote prefix if required for testing,
        # otherwise None.
        return BUCKET_NAME


def get_default_service_account_email():
    """Returns the default service account if running on a GCE VM, otherwise None."""
    response = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email",
        headers={"Metadata-Flavor": "Google"},
    )
    if response.status_code == requests.codes.ok:
        return response.text
    else:
        return None
    

def cleanup_google_storage():
    """Given a storage prefix and a bucket, recursively delete files there
    This is intended to run after testing to ensure that
    the bucket is cleaned up.
    """
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()
    for blob in blobs:
        blob.delete()

def create_google_storage(bucket_name="snakemake-testing"):
    """Given a bucket name, create the Google storage bucket,
    intending to be used for testing and then cleaned up by
    cleanup_google_storage

    Arguments:
      bucket_name (str) : the name of the bucket, default snakemake-testing
    """
    client = storage.Client()
    return client.create_bucket(bucket_name)