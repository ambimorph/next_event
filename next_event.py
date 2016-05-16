from collections import defaultdict

class circular_array_of_timestamps(object):

    def __init__(self, size):

        # The maximum number of timestamps
        self.size = size
        # Maps the id the data is associated with to the real timestamp
        self.key_to_timestamp = {}
        # Maps the bucket to the current real timestamp
        self.bucket_to_timestamp  = {}
        for i in range(size):
            self.bucket_to_timestamp[i] = None
        # Maps a timpestamp bucket to a dict of key:data
        self.buckets = defaultdict(dict)

    def get_bucket(self, timestamp):
        return timestamp % self.size

    def insert(self, key, data, timestamp):
        msg = "Key already in dict; merge and delete old data first"
        assert not self.key_to_timestamp.has_key(key), msg
        self.key_to_timestamp[key] = timestamp
        bucket = self.get_bucket(timestamp)
        if self.bucket_to_timestamp.get(bucket, None) != timestamp:
            self.replace_bucket(timestamp)
        self.buckets[bucket][key] = data

    def pop(self, key):
        # atomic fetch and delete
        t = self.key_to_timestamp.pop(key)
        bucket = self.get_bucket(t)
        return (self.buckets[bucket].pop(key),
                self.bucket_to_timestamp[bucket])

    def replace_bucket(self, timestamp):
        # create or update bucket to current timestamp
        self.buckets[self.get_bucket(timestamp)] = {}
        self.bucket_to_timestamp[self.get_bucket(timestamp)] = timestamp

    def get_old_record(self, key, timestamp):

        if self.key_to_timestamp.has_key(key):
           return self.pop(key)

    def generate_timed_out_events(self, timestamp):
        bucket = self.get_bucket(timestamp)
        for k, v in self.buckets[bucket].iteritems():
            yield (k, v, self.bucket_to_timestamp[bucket], None)

    def process_record(self, key, data, timestamp):

        try:
            old_data, old_timestamp = self.get_old_record(key, timestamp)
            full_record = (key, old_data, old_timestamp, timestamp)
        except TypeError:
            full_record = None

        if timestamp > self.bucket_to_timestamp[self.get_bucket(timestamp)]:
            for e in self.generate_timed_out_events(timestamp):
                yield e
            self.replace_bucket(timestamp)

        self.insert(key, data, timestamp)

        yield full_record
