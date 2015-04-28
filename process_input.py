from multiprocessing import Pool, cpu_count
from collections import defaultdict, namedtuple
import ujson as json

PoolSize = cpu_count() / 2


# Using namedtuples will help save memory when dealing with many objects
Entry = namedtuple('Entry', 'unique_id, from_id, online, is_message')
TimestampEntry = namedtuple('TimestampEntry', 'timestamp, entry')
SiteTimestampEntry = namedtuple('SiteTimestampEntry',
                                'site_id, timestampentry')


def process_site(id_timestamp_entries):
    """Returns a tupe that looks like this:
       ('124', 'messages=2,emails=1,operators=4,visitors=1')

    Expects that entries will be a list of (timestamp, Entry) tuples
    """
    site_id, timestamp_entries = id_timestamp_entries
    online_operators = set()
    seen_operators = set()
    seen_visitors = set()
    seen_messages = set()
    sent_messages = 0
    sent_emails = 0
    for timestamp_entry in sorted(timestamp_entries):
        entry = timestamp_entry.entry
        if entry.unique_id in seen_messages:
            continue
        seen_messages.add(entry.unique_id)
        if entry.is_message:
            seen_visitors.add(entry.from_id)
            if online_operators:
                sent_messages += 1
            else:
                sent_emails += 1
        else:
            if entry.online:
                online_operators.add(entry.from_id)
            elif entry.from_id in online_operators:
                online_operators.remove(entry.from_id)
            seen_operators.add(entry.from_id)
    # The sort needs to be alphanumeric so we return the id as a string
    return (str(site_id),
            'messages={},emails={},operators={},visitors={}'.format(
                sent_messages, sent_emails, len(seen_operators),
                len(seen_visitors)))


def parse_entry(raw_entry):
    entry = json.loads(raw_entry)
    return SiteTimestampEntry(
        site_id=entry['site_id'],
        timestampentry=TimestampEntry(
            timestamp=entry['timestamp'],
            entry=Entry(
                is_message=(entry['type'] == 'message'),
                online=entry.get('data', {}).get('status', '') == 'online',
                unique_id=entry['id'],
                from_id=entry['from'])))


# assert parse_entry('{"id":"7f360d94-ecae-4d8e-a26a-e11cea7bde2b","type":"message","from":"visitor5","site_id"'
#                       ':"1","timestamp":4301,"data":{"message":"sorting than been"}}') == (4301, Entry(is_message=True, online=False, unique_id='7f360d94-ecae-4d8e-a26a-e11cea7bde2b', from_id='visitor5'))
# assert parse_entry('{"id":"f0d6ebe9-4830-415b-a826-a6ef47ebb649","type":"status","from":"operator1","site_id":"1","timestamp":5630,"data":{"status":"online"}}') == (5630, Entry(is_message=False, online=True, unique_id='f0d6ebe9-4830-415b-a826-a6ef47ebb649', from_id='operator1'))

if __name__ == '__main__':
    p = Pool(PoolSize)
    site_data = defaultdict(lambda: [])
    for line in open('big_input').readlines():
        sitetimestampentry = parse_entry(line)
        site_data[sitetimestampentry.site_id].append(sitetimestampentry.timestampentry)

    for site_id, results in sorted(p.imap_unordered(process_site, site_data.items())):
        print '{},{}'.format(site_id, results)
