from multiprocessing import Pool, cpu_count
import sqlite3
import sys
from threading import local
import ujson as json

POOL_SIZE = cpu_count()
threadlocal = local()
threadlocal.read_connection = None


def process_site(site_id):
    """Returns the string used in reports:
       '124,messages=2,emails=1,operators=4,visitors=1'
    """
    online_operators = set()
    seen_operators = set()
    seen_visitors = set()
    sent_messages = 0
    sent_emails = 0
    if not threadlocal.read_connection:
        threadlocal.read_connection = sqlite3.connect('work.db')
    cursor = threadlocal.read_connection.cursor()
    entries = cursor.execute(
        'SELECT DISTINCT _timestamp, _id, is_message, _from, online '
        'FROM entries '
        'WHERE site_id = ? '
        'ORDER BY _timestamp', (site_id,))
    for timestamp, _id, is_message, _from, online in entries:
        if is_message:
            seen_visitors.add(_from)
            if online_operators:
                sent_messages += 1
            else:
                sent_emails += 1
        else:
            if online:
                online_operators.add(_from)
            elif _from in online_operators:
                online_operators.remove(_from)
            seen_operators.add(_from)
    return ('{},messages={},emails={},operators={},visitors={}'.format(
                site_id, sent_messages, sent_emails, len(seen_operators),
                len(seen_visitors)))


if __name__ == '__main__':
    connection = sqlite3.connect('work.db')
    cursor = connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS entries')
    cursor.execute('CREATE TABLE entries (site_id TEXT, _timestamp INTEGER, '
                   '_id INTEGER, is_message INTEGER, _from TEXT, '
                   'online INTEGER)')
    cursor.execute('CREATE INDEX site_id_timestamp_idx '
                   'ON entries (site_id, _timestamp)')
    for line in sys.stdin:
        entry = json.loads(line)
        cursor.execute('INSERT INTO entries VALUES (?,?,?,?,?,?)', (
            entry['site_id'],
            entry['timestamp'],
            entry['id'],
            entry['type'] == 'message' and 1 or 0,
            entry['from'],
            entry.get('data', {}).get('status', '') == 'online' and 1 or 0
        ))

    connection.commit()
    connection.close()
    read_connection = sqlite3.connect('work.db')
    pool = Pool(POOL_SIZE)
    cursor = read_connection.cursor()
    site_ids = cursor.execute('SELECT DISTINCT site_id FROM entries '
                              'ORDER BY site_id')
    for response in pool.imap(process_site,
                              (s[0] for s in site_ids.fetchall())):
        print response
