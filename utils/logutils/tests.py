import unittest
from logutils import LogUtils
import os
import json
import time

logconfig = """
{
    "loglevel" : "DEBUG",
    "format" : "[%(asctime)s] %(message)s: %(levelname)s",
    "dateFormat" : "%m/%d/%Y %I:%M:%S %p"
}

"""


class TestLogUtils(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        print('Tearing down')
        pass

    def test_logwrite_config(self):
        logger = LogUtils("testlogger_cnf", json.loads(logconfig)).get_logger()
        logger.error("Test log message")
 #       import pdb; pdb.set_trace()
        logfile = open('testlogger_cnf.log','r')
        logstr = logfile.read()
        logfile.close()
    #    os.remove('testlogger_cnf.log')
        self.assertTrue(logstr.find('Test log message') > -1)
        self.assertTrue(logstr.find('ERROR') > -1)
        self.assertTrue(logstr.find('ERROR') > logstr.find('Test log message'))


    def test_logwrite_befault(self):
        logger = LogUtils("testlogger_def").get_logger()
        logger.warning("Test log message")
        logfile = open('testlogger_def.log','r')
        logstr = logfile.read()
        logfile.close()
    #    os.remove('testlogger_def.log')
        self.assertTrue(logstr.find('Test log message') > -1) 
        self.assertTrue(logstr.find('WARNING') > -1)
        self.assertTrue(logstr.find('WARNING') < logstr.find('Test log message'))



if __name__ == '__main__':
    unittest.main()
