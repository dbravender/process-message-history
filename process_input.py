from multiprocessing import Pool, cpu_count
from collections import defaultdict
import ujson as json

POOL_SIZE = cpu_count() / 2
entries = defaultdict(lambda: [])


def process_site(site_id):
    """Returns the string used in reports:
       '124,messages=2,emails=1,operators=4,visitors=1'
    """
    online_operators = set()
    seen_operators = set()
    seen_visitors = set()
    seen_messages = set()
    sent_messages = 0
    sent_emails = 0
    for timestamp, _id, is_message, _from, online in sorted(entries[site_id]):
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
    site_ids = set()
    for line in open('big_input').readlines():
        entry = json.loads(line)
        site_id = str(entry['site_id'])
        entries[site_id].append((
            entry['timestamp'],
            entry['id'],
            entry['type'] == 'message',
            entry['from'],
            entry.get('data', {}).get('status', '') == 'online',
        ))
        site_ids.add(site_id)

    # fork here so the child processes each have a copy of the entries
    # dictionary
    p = Pool(POOL_SIZE)
    for response in p.map(process_site, sorted(site_ids)):
        print response
