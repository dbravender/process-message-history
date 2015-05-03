from multiprocessing import Pool, cpu_count
from glob import iglob
import numpy as np
import ujson as json

POOL_SIZE = cpu_count() / 2


def process_site(work_path):
    """Returns the string used in reports:
       '124,messages=2,emails=1,operators=4,visitors=1'
    """
    site_id = work_path[5:]
    online_operators = set()
    seen_operators = set()
    seen_visitors = set()
    seen_messages = set()
    sent_messages = 0
    sent_emails = 0
    entries = np.genfromtxt(work_path, delimiter=',', dtype=None)
    entries.sort()
    for timestamp, _id, is_message, _from, online in entries:
        if _id in seen_messages:
            continue
        seen_messages.add(_id)
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
    pool = Pool(POOL_SIZE)
    for line in open('big_input'):
        entry = json.loads(line)
        is_message = entry['type'] == 'message' and 1 or 0
        csv_line = '{},{},{},{},{}\n'.format(
            entry['timestamp'],
            entry['id'],
            is_message,
            entry['from'],
            entry.get('data', {}).get('status', '') == 'online' and 1 or 0
        )
        open('work/{}'.format(entry['site_id']), 'a').write(csv_line)

    for response in pool.imap_unordered(process_site, iglob('work/*')):
        print response
