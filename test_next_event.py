import next_event, unittest


class NextEventTest(unittest.TestCase):

    def setUp(self):

        self.ne = next_event.circular_array_of_timestamps(3)
        self.events = [
            ("0", "a little data", 30),
            ("1", "some stuff", 30),
            ("2", "other", 31),
            ("1", "new stuff", 32),
            ("0", "more data", 33),
            ("3", "something else", 33),
        ]

    def test_insert(self):

        self.ne.insert(*self.events[0])
        self.assertDictEqual({"0":30}, self.ne.key_to_timestamp)
        self.assertDictEqual({0:30,
                              1:None,
                              2:None}, self.ne.bucket_to_timestamp)
        self.assertDictEqual({0:{"0":"a little data"}}, 
                             self.ne.buckets)

        self.ne.insert(*self.events[1])
        self.assertDictEqual({"0":30, "1":30}, self.ne.key_to_timestamp)
        self.assertDictEqual({0:30,
                              1:None,
                              2:None}, self.ne.bucket_to_timestamp)
        self.assertDictEqual({0:{"0":"a little data",
                                 "1":"some stuff"}}, 
                             self.ne.buckets)

        with self.assertRaises(Exception):
            self.ne.insert(self.events[3])

    def test_pop(self):

        self.ne.insert(*self.events[0])
        d, t = self.ne.pop("0") 
        self.assertEquals("a little data", d)
        self.assertEquals(30, t)
        self.assertDictEqual({}, self.ne.key_to_timestamp)
        self.assertDictEqual({}, self.ne.buckets[0])
        
    def test_replace_bucket(self):

        self.ne.insert(*self.events[0])
        self.assertDictEqual({0:{"0":"a little data"}}, 
                             self.ne.buckets)
        self.ne.replace_bucket(33)
        self.assertDictEqual({}, self.ne.buckets[0])
        
    def test_get_old_record(self):

        self.ne.insert(*self.events[1])
        d, t = self.ne.get_old_record(self.events[1][0], self.events[1][2])
        self.assertEquals("some stuff", d)
        self.assertEquals(30, t)
        self.assertDictEqual({}, self.ne.key_to_timestamp)
        self.assertDictEqual({}, self.ne.buckets[1])

    def test_process_record(self):

        full_records = []
        for e in self.events:
            result = self.ne.process_record(*e)
            if result is not None:
                full_records.append(result)

        self.assertListEqual([("1", "some stuff", 30, 32),
                              ("0", "a little data", 30, 33)], full_records)
        
        self.assertDictEqual({"0":33, "1":32, "2":31, "3":33}, self.ne.key_to_timestamp)
        self.assertDictEqual({0:33,
                              1:31,
                              2:32}, self.ne.bucket_to_timestamp)
        self.assertDictEqual({0:{"0":"more data", "3":"something else"},
                              1: {"2":"other"},
                              2: {"1":"new stuff"}},
                             self.ne.buckets)
        
        

if __name__ == '__main__':
    unittest.main()
