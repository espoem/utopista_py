import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import date, timedelta

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pyArango.connection import Connection

import sheet

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
fh = logging.FileHandler(f"{DIR_PATH}/utopian-db.log")
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

with open(f'{DIR_PATH}/config.json', 'r') as f:
    CONFIG = json.load(f)

SCOPE = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_name(
        f'{DIR_PATH}/credentials.json', SCOPE)
CLIENT = gspread.authorize(CREDENTIALS)
SHEET = CLIENT.open_by_key(CONFIG['google_sheet']['key'])

REVIEW_FIRST_DAY = date(2018, 5, 3)

CONN = Connection(username=CONFIG['db']['username'],
        password=CONFIG['db']['password'])
if not CONN.hasDatabase('utopian'):
    CONN.createDatabase('utopian')

DB = CONN['utopian']

POSTS_COLLECTION = 'posts'


def connect_collection(db, col_name):
    if not db.hasCollection(name=col_name):
        db.createCollection(name=col_name)
    return db[col_name]


postCol = connect_collection(DB, POSTS_COLLECTION)


def find_document(collection, dataExample: dict, asDict=True):
    return collection.fetchFirstExample(dataExample, rawResults=asDict)


def save_document(collection, document: dict):
    logger.info(
            f'Saving document {document["author"]} - {document["permlink"]}')
    d = collection.createDocument(document)
    d.save(waitForSync=True)
    return d


def update_document(db, key: str, document: dict, col_name: str):
    # logger.info(
    #         f'Updating document {document["_key"]} - {document["author"]} - {document["permlink"]}')
    aql = "UPDATE @key WITH @doc IN @@col OPTIONS {waitForSync: true} " \
          "RETURN NEW"
    bind = {'key': key, 'doc': document, '@col': col_name}
    result = db.AQLQuery(aql, bindVars=bind)
    if result:
        return result[0]
    return None


def process_post_db(post):
    # logger.info(f'Processing document {post["author"]} - {post["permlink"]}')
    p = find_document(postCol,
            {'author': post['author'], 'permlink': post['permlink']})
    print(post['author'], post['permlink'])
    if p:
        p = p[0]
        new = update_document(DB, p['_key'], post, POSTS_COLLECTION)
    else:
        new = save_document(postCol, post)
    return new


def update_db(update=True):
    """Update DB with posts. If update is True, then updates posts from last
    2 weeks, else update all posts.

    :param update: update flag
    """
    if update:
        cur_week = sheet.get_reviewed_posts_in_week(SHEET, date.today())
        prev_week = sheet.get_reviewed_posts_in_week(SHEET,
                date.today() - timedelta(7))
        unreviewed = sheet.get_unreviewed_posts(SHEET)
    else:
        cur_week = sheet.get_all_posts(SHEET)
        prev_week = []
        unreviewed = []

    futures = []
    with ThreadPoolExecutor() as ex:
        for post in prev_week:
            futures.append(ex.submit(process_post_db, post))

        for post in cur_week:
            futures.append(ex.submit(process_post_db, post))

        for post in unreviewed:
            futures.append(ex.submit(process_post_db, post))

    wait(futures)


if __name__ == '__main__':
    update_db(update=True)
