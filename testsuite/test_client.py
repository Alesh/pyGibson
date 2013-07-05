import time
import unittest
import gibson.client

servers = ["/home/research/gibson.sock"]

class test_Client(unittest.TestCase):
    
    def setUp(self):
        self.client = gibson.client.Client(servers)
    
    def test_Connected(self):
        self.assertEqual(self.client.connected, True)

    def test_SetGet(self):
        self.client.set('KeyA', 'OK', 1)
        self.assertEqual(self.client.get('KeyA'), 'OK')

    def test_SetGetDelGet(self):
        self.client.set('KeyA', 'OK', 1)
        self.assertEqual(self.client.get('KeyA'), 'OK')
        self.assertEqual(self.client.delete('KeyA'), True)
        self.assertEqual(self.client.get('KeyA'), None)

    def test_SetIncDec(self):
        self.client.set('KeyN', 0, 1)
        self.assertEqual(self.client.inc('KeyN'), 1)
        self.assertEqual(self.client.inc('KeyN'), 2)
        self.assertEqual(self.client.inc('KeyN'), 3)
        self.assertEqual(self.client.get('KeyN'), 3)
        self.assertEqual(self.client.dec('KeyN'), 2)
        self.assertEqual(self.client.dec('KeyN'), 1)
        self.assertEqual(self.client.dec('KeyN'), 0)
        self.assertEqual(self.client.get('KeyN'), 0)
        self.assertEqual(self.client.dec('KeyN'), -1)
        self.assertEqual(self.client.dec('KeyN'), -2)
        self.assertEqual(self.client.get('KeyN'), -2)
        
    #def test_TTL(self):
    #    self.client.set('KeyT', 'OK', 3)
    #    time.sleep(0.5)
    #    self.assertEqual(self.client.get('KeyT'), 'OK')
    #    time.sleep(0.6)
    #    self.assertEqual(self.client.get('KeyT'), None)

    #def test_TTL2(self):
    #    self.client.set('KeyT2', 'OK', 1)
    #    time.sleep(0.5)
    #    self.assertEqual(self.client.get('KeyT2'), 'OK')
    #    self.assertEqual(self.client.ttl('KeyT2', 2), True)
    #    time.sleep(0.6)
    #    self.assertEqual(self.client.get('KeyT2'), 'OK')
    
    def tearDown(self):
        self.client.close()


if __name__ == '__main__':
    unittest.main()