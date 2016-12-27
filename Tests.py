import unittest
import time
import os
import csv
from Updater import MdbDistillator, Spider

class Tests(unittest.TestCase):

    def test_files_removing(self):
        distillator = MdbDistillator()
        distillator.get_rows()
        self.assertEqual(len([file for file in os.listdir('Temporary')]), 0, 'Files were deleted')

    def test_rows_not_empty(self):
        distillator = MdbDistillator()
        rows = distillator.get_rows()
        self.assertNotEqual(len(rows), 0, 'Rows were extracted')

    def test_extraction(self):
        distillator = MdbDistillator()
        rows = distillator.get_rows()
        spider = Spider()
        try:
            for i in [5, 100]:
                start = time.time()
                spider.scrape(spider.load_items(rows, slice=i))
                end = time.time()
                print('Spider ' + str(i) + ' ended in ' + str(end - start))
        except Exception as e:
            self.fail('test extraction failed')

if __name__ == '__main__':
    unittest.main()