import json
import logging
import time

from tinydb import TinyDB, Query
import requests
import urllib3
import re
import sys


class CustomFormatter(logging.Formatter):
    def __init__(self):
        super(CustomFormatter, self).__init__()

    def format(self, record: logging.LogRecord) -> str:
        match record.levelno:
            case logging.INFO:
                self._style._fmt = "INFO - %(asctime)s - %(msg)s"
            case logging.ERROR:
                self._style._fmt = f"ERROR - %(asctime)s - %(msg)s - on line: {sys.exc_info()[-1].tb_lineno}"
            case logging.WARNING:
                self._style._fmt = "WARNING - %(asctime)s - %(msg)s"

        return super().format(record)


def init_logger():
    global smart_logger, constants

    smart_logger.setLevel(logging.INFO)
    smart_formatter = CustomFormatter()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(smart_formatter)

    file_handler = logging.FileHandler(constants['log_path'], encoding='utf-8')
    file_handler.setFormatter(smart_formatter)

    smart_logger.addHandler(console_handler)
    smart_logger.addHandler(file_handler)


def request_request(method, url, **additional_params):
    response = None
    retries = 0

    while retries < max_retries:
        if retries > 0:
            smart_logger.info(f"Retrying request in 1 minute, attempt {retries + 1} out of {max_retries}.")
            time.sleep(60)

        try:
            response = requests.request(method, url, verify=False, **additional_params)
            response.raise_for_status()

            return response.json()
        except requests.exceptions.HTTPError as err_h:
            if "A page with this title already exists:" in response.text:
                raise requests.exceptions.HTTPError
            else:
                smart_logger.error(err_h)
        except requests.exceptions.ConnectionError as err_c:
            smart_logger.error(err_c)
        except requests.exceptions.Timeout as err_t:
            smart_logger.error(err_t)
        except requests.exceptions.RequestException as err:
            if response.status_code == 204:
                return
            smart_logger.error(err)
        except Exception as e:
            smart_logger.error(e)

        retries += 1
    try:
        raise requests.exceptions.RequestException
    except requests.exceptions.RequestException:
        smart_logger.error(
            "Max retries reached, in the next run the system will try to run from this point on. exiting...")
        sys.exit()


def get_page_data(page):
    global constants, auth_details, smart_logger

    url = constants['host'] + 'rest/api/content'

    query = {
        "type": "page",
        "spaceKey": constants['space_key'],
        "title": page
    }

    smart_logger.info(f'Starting to work on: "{page}"')

    return request_request(
        "GET",
        url,
        params=query,
        auth=auth_details,
    )['results'][0]


def get_page_label_name(page_name):
    invalid_cars = ['(', '!', '#', '&', '(', ')', '*', '.', ':', ';', '<', '>', '?', '@', '[', ']', '^', ',', '-']
    label = page_name.strip()
    label = label.translate({ord(char): ' ' for char in invalid_cars})
    label = '_'.join(label.split())

    return label


def get_children(url):
    global auth_details, constants

    children = []
    has_more = True
    url = constants['host'] + url[1:] + '/page'

    while has_more:
        data = request_request(
            "GET",
            url,
            auth=auth_details,
        )
        children.extend(data['results'])

        if 'next' in data['_links']:
            url = constants['host'] + data['_links']['next'][1:]
        else:
            has_more = False

    return children


def is_file(url):
    global auth_details, constants

    url = constants['host'] + url[1:] + '/attachment'

    response = request_request(
        "GET",
        url,
        auth=auth_details,
    )

    return len(response['results']) != 0


def get_page_labels(page_id):
    global constants, auth_details

    url = constants['host'] + f'rest/api/content/{page_id}/label'

    return request_request(
        "GET",
        url,
        auth=auth_details
    )['results']


def delete_labels(page_id):
    global constants, auth_details, smart_logger
    labels_to_delete = get_page_labels(page_id)

    url = constants['host'] + f'rest/api/content/{page_id}/label'

    smart_logger.info("Deleting page's labels before adding new ones.")

    for label in labels_to_delete:
        request_request(
            "DELETE",
            url,
            params={"name": label['name']},
            auth=auth_details
        )


def add_labels(page_id, labels):
    global constants, auth_details

    url = constants['host'] + f'rest/api/content/{page_id}/label'
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    for label in labels:
        payload = [{"name": label}]
        smart_logger.info(f'Adding label: "{label}"')

        request_request(
            "POST",
            url,
            json=payload,
            headers=headers,
            auth=auth_details
        )


def cache_page(page_id, page_name):
    global db, smart_logger

    smart_logger.info(f"Caching page '{page_name}'")
    db.insert({"id": page_id})


def is_cached(page_id: str):
    global db, Cache
    result = db.search(Cache.id == page_id)

    return len(result) != 0


def fix_label(page, parent_id=""):
    global smart_logger
    page_data = get_page_data(page)
    page_id = page_data['id']
    children_url = page_data['_expandable']['children']

    if is_cached(page_id):
        smart_logger.info("Page already cached, skipping...")
    else:
        delete_labels(page_id)

        """ If not root, load parent labels"""
        if parent_id:
            smart_logger.info("Loading parent labels.")
            parent_labels = get_page_labels(parent_id)
            add_labels(page_id, [label['name'] for label in parent_labels])

        """ Check if file, if so finish"""
        if is_file(children_url):
            smart_logger.info(f' "{page}" is a file, no need to add additional label or go deeper.')
            return

        new_label = get_page_label_name(page)
        smart_logger.info(f'Generating label for current page: "{new_label}"')

        """ Check if title is enumerated """
        if re.search(r"^.* - #\d+$", page):
            new_label = new_label.split("_")[:-1]
            new_label = "_".join(new_label)
            smart_logger.info(f'Page title is enumerated, regenerating label: "{new_label}"')

        add_labels(page_id, [new_label])
        cache_page(page_id, page)

        smart_logger.info(f'Finished working on: "{page}", fetching children.')
    for child in get_children(children_url):
        smart_logger.info(f"Going deeper from: \"{page}\" to: \"{child['title']}\"")
        fix_label(child['title'], page_id)


if __name__ == '__main__':
    max_retries = 60 * 5  # 5 Hours
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    with open('constants.json', encoding="utf8") as const_file:
        constants = json.load(const_file)

    if constants['host'][-1] != '/':
        constants['host'] += '/'

    smart_logger = logging.getLogger()
    auth_details = (constants['username'], constants['password'])
    db = TinyDB(constants['db'])
    Cache = Query()

    try:
        init_logger()
        fix_label(constants['root_page_on_confluence'])

        smart_logger.info("RUN COMPLETED!")
        smart_logger.info("--------------")
    except Exception as e:
        smart_logger.error("GENERAL ERROR OCCURRED:")
        smart_logger.error(e)
