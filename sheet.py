import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import gspread
from beem.comment import Comment
from dateutil.parser import parser
from oauth2client.service_account import ServiceAccountCredentials

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

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


def parse_date_to_iso(date: str):
    """

    :param date: date string to parse
    :return: parsed date in ISO format
    """
    if not date:
        return ''

    try:
        parsed_date = parser.parse(parser(), date).isoformat()
    except ValueError:
        try:
            parsed_date = parser.parse(parser(), date,
                    dayfirst=True).isoformat()
        except ValueError:
            parsed_date = ''
    except Exception:
        parsed_date = ''

    return parsed_date


def get_status(row):
    """Get contribution status from a worksheet row.
    statuses :: reviewed, pending, rejected

    :param row: list of values from worksheet
    :return: contribution status
    """
    row_status = row[9].lower()

    if row_status == 'yes' or row_status == 'pending':
        status = 'reviewed'
    elif not (row[0] and row[1] and row[5]):
        status = 'pending'
    else:
        status = 'rejected'

    return status


def get_utopian_vote(post: Comment):
    """Get information about Utopian vote.

    :param post: post data from blockchain
    :return: dict with the vote information
    """
    voters = post.json()['active_votes']
    for voter in voters:
        if voter['voter'] == 'utopian-io':
            return voter
    return None


def contribution(row):
    """Create a dictionary for a contribution post.

    :param row: list of values from worksheet
    :return: dict for contribution
    """
    url = row[2]
    url_split = url.split('/')
    author = url_split[4][1:]
    permlink = url_split[5]
    review_date = parse_date_to_iso(row[1])
    repo_split = row[3].split('/') if 'github.com' in row[3] else []
    staff_pick = {
        'picked_by': row[8],
        'date': parse_date_to_iso(row[7])
    } if row[6].lower() == 'yes' else None

    repo_full_name = f'{repo_split[3]}/{repo_split[4]}' \
        if len(repo_split) > 4 else ''
    score = float(row[5]) if row[5] else 0
    category = row[4]

    post = Comment(f'@{author}/{permlink}')
    post_meta = post.json_metadata
    post_json = post.json()
    created = parse_date_to_iso(post_json['created'])

    contrib = {
        'author': author,
        'permlink': permlink,
        'post_category': (url_split[3]),
        'moderator': {
            'account': (row[0]),
            'date': review_date
        },
        'repository': {
            'full_name': repo_full_name,
            'html_url': (
                f'https://github.com/{repo_full_name}' if repo_full_name else '')
        },
        'score': score,
        'status': get_status(row),
        'category': category or (
            post_meta.get('tags')[1] if len(post_meta.get('tags')) > 1 else ''),
        'tags': (post_meta.get('tags')),
        'created': created,
        'body': (post_json.get('body', '')),
        'utopian_vote': (get_utopian_vote(post)),
        'staff_pick': staff_pick
    }

    return contrib


def user(row):
    assert len(row) > 4
    banned_since = parse_date_to_iso(row[2]) if row[2] else ''
    banned_until = parser.parse(parser(), banned_since) + timedelta(
            days=float(row[1])) if banned_since else ''
    user = {
        'account': row[0] or '',
        'is_banned': row[3].lower() == 'yes',
        'banned_since': banned_since,
        'banned_until': banned_until.isoformat() if banned_until else '',
        'banned_by': row[5],
        'reason': row[4]
    }
    return user


def get_watched_users(sheet: gspread.models.Spreadsheet, bannedOnly=True):
    """Get all banned users from Banned users worksheet.

    :param bannedOnly:
    :param sheet: utopian review sheet
    :return: generator object of watched users
    """
    banned_sheet = sheet.worksheet('Banned users')
    return (user(row) for row in banned_sheet.get_all_values()[1:]
        if row and row[0] and (row[3].lower() == 'yes' if bannedOnly else True))


def get_unreviewed_posts(sheet: gspread.models.Spreadsheet):
    """Get all unreviewed posts.

    :param sheet: Google spreadsheet
    :return: generator object of not yet reviewed posts
    """
    week = get_review_week_start_end(date.today())
    title = f"Unreviewed - {week[0]:%b} {week[0].day} - {week[1]:%b} {week[1].day}"
    unreviewed = sheet.worksheet(title)
    return (contribution(row) for row in unreviewed.get_all_values()[1:] if row)


def get_unreviewed_reserved_posts(sheet: gspread.models.Spreadsheet):
    """Get all posts that are reserved by moderators

    :param sheet: Google spreadsheet
    :return: generator object of not yet reviewed posts that are reserved
    """
    return (contribution(row) for row in get_unreviewed_posts(sheet) if
        row and len(row[0]) > 0)


def get_reviewed_posts_in_week(sheet: gspread.models.Spreadsheet,
        week_date: date):
    """Get reviewed contributions in one week.
    The week is decided by the given date.
    Review week starts on Thursday.

    :param sheet: Google spreadsheet
    :param week_date: date of the week
    :return: generator object of the contributions
    """
    week = get_review_week_start_end(week_date)
    title = f"Reviewed - {week[0]:%b} {week[0].day} - {week[1]:%b} {week[1].day}"
    reviewed = sheet.worksheet(title)
    return (contribution(row) for row in reviewed.get_all_values()[1:] if row)


def get_all_reviewed_posts(sheet: gspread.models.Spreadsheet):
    """Get all reviewed contributions.

    :param sheet: Google spreadsheet
    """
    cur_date = REVIEW_FIRST_DAY
    futures = []
    today = date.today()
    with ThreadPoolExecutor() as executor:
        while cur_date <= today:
            futures.append(
                    executor.submit(get_reviewed_posts_in_week, sheet,
                            cur_date))
            cur_date += timedelta(7)

    for res in as_completed(futures):
        for row in res.result():
            yield row


def get_all_posts(sheet):
    """Get all contributions. Includes reviewed and not yet reviewed contributions.

    :param sheet: Google spreadsheet
    """
    reviewed = get_all_reviewed_posts(sheet)
    unreviewed = get_unreviewed_posts(sheet)

    for row in reviewed:
        yield row

    for row in unreviewed:
        yield row


def get_review_week_start_end(date_in_week: date):
    """Get a particular review week with a given date. The week starts on Thursday.

    :param date_in_week: date
    :return: Dates of the first and last day of the week.
    """
    offset = (date_in_week.weekday() - 3) % 7
    this_week = date_in_week - timedelta(days=offset)
    next_week = this_week + timedelta(days=7)
    return this_week, next_week
