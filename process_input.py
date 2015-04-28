from multiprocessing import Pool, cpu_count
from collections import defaultdict, namedtuple
import ujson as json

POOL_SIZE = cpu_count()

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
    for timestamp, _id, _from, online in sorted(entries[site_id]):
        if _id in seen_messages:
            continue
        seen_messages.add(_id)
        if entry.is_message:
            seen_visitors.add(entry.from_id)
            if online_operators:
                sent_messages += 1
            else:
                sent_emails += 1
        else:
            if online:
                online_operators.add(_from)
            elif entry.from_id in online_operators:
                online_operators.remove(_from)
            seen_operators.add(entry.from_id)
    # The sort needs to be alphanumeric so we return the id as a string
    return ('{},messages={},emails={},operators={},visitors={}'.format(
                site_id, sent_messages, sent_emails, len(seen_operators),
                len(seen_visitors)))


# assert parse_entry('{"id":"7f360d94-ecae-4d8e-a26a-e11cea7bde2b","type":"message","from":"visitor5","site_id"'
#                       ':"1","timestamp":4301,"data":{"message":"sorting than been"}}') == (4301, Entry(is_message=True, online=False, unique_id='7f360d94-ecae-4d8e-a26a-e11cea7bde2b', from_id='visitor5'))
# assert parse_entry('{"id":"f0d6ebe9-4830-415b-a826-a6ef47ebb649","type":"status","from":"operator1","site_id":"1","timestamp":5630,"data":{"status":"online"}}') == (5630, Entry(is_message=False, online=True, unique_id='f0d6ebe9-4830-415b-a826-a6ef47ebb649', from_id='operator1'))

if __name__ == '__main__':
    p = Pool(POOL_SIZE)
    site_ids = []
    for line in open('big_input').readlines():
        entry = json.loads(line)
        site_id = str(entry['site_id'])
        entries[site_id].append((
            entry['timestamp'],
            entry['id'],
            entry['type'] == 'message',
            entry['from'],
            int(entry.get('data', {}).get('status', '') == 'online'),
        ))
        site_ids.append(site_id)

    for response in p.imap(process_site, sorted(site_ids)):
        print response
