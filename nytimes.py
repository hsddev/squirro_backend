import argparse
import logging
import requests
from pprint import pprint

"""
Skeleton for Squirro Delivery Hiring Coding Challenge
August 2021
"""


log = logging.getLogger(__name__)


class NYTimesSource(object):
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(self):
        pass

    def connect(self, inc_column=None, max_inc_value=None):
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass

    def getDataBatch(self, parameters):
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        """
    
        url = "https://api.nytimes.com/svc/search/v2/articlesearch.json"

        headers = {}
        while(True):
            try:
                response = requests.get(url,headers=headers,params=parameters)
                yield response.json()["response"]["docs"]
            except:
                break    

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """

        schema = [
            "title",
            "body",
            "created_at",
            "id",
            "summary",
            "abstract",
            "keywords",
        ]

        return schema

def flatten_dict(dictionary, accumulator=None, parent_key=None, separator="."):
    if accumulator is None:
        accumulator = {}

    for k, v in dictionary.items():
        k = f"{parent_key}{separator}{k}" if parent_key else k
        if isinstance(v, dict):
            flatten_dict(dictionary=v, accumulator=accumulator, parent_key=k)
            continue

        accumulator[k] = v

    return accumulator



if __name__ == "__main__":
    config = {
    'q': 'Silicon Valley',
    'api-key': '5xGQDfCcgKhSiLwrHFJR1kbF9Gxo8E0w',
    }
    source = NYTimesSource()
    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    
    for idx, batch in enumerate(source.getDataBatch(config)):
        print(f"{idx} Batch of {len(batch)} items")
        for item in batch:
            pprint(flatten_dict(item))