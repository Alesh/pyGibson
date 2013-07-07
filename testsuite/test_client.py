import time
import unittest
import logging
import gibson

servers = ["/tmp/gibson1.sock",
           "/tmp/gibson2.sock",
           "/tmp/gibson3.sock",
           "/tmp/gibson4.sock"]

class test_Client(unittest.TestCase):
    
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        self.client = gibson.Client(servers, debug=True)
    
    def test_Connected(self):
        self.assertEqual(self.client.connected, True)
    
    def test_SetGet(self):
        self.client.set('Key', 'OK', 1)
        self.assertEqual(self.client.get('Key'), 'OK')
    
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
        
    def test_TTL(self):
        self.client.set('KeyT', 'OK', 2)
        time.sleep(1)
        self.assertEqual(self.client.get('KeyT'), 'OK')
        time.sleep(1)
        self.assertEqual(self.client.get('KeyT'), None)
    
    def test_TTL2(self):
        self.client.set('KeyT2', 'OK', 2)
        time.sleep(1)
        self.assertEqual(self.client.get('KeyT2'), 'OK')
        self.assertEqual(self.client.ttl('KeyT2', 2), True)
        time.sleep(1)
        self.assertEqual(self.client.get('KeyT2'), 'OK')
        
    
    def test_LockUnlock(self):
        self.client.set('KeyL', 'OK', 3)
        self.assertEqual(self.client.get('KeyL'), 'OK')
        self.assertTrue(self.client.lock('KeyL', 1))
        self.assertFalse(self.client.lock('KeyL', 1))
        time.sleep(1)
        self.assertTrue(self.client.lock('KeyL', 1))
        time.sleep(1)
        self.assertEqual(self.client.get('KeyL'), 'OK')
        self.assertTrue(self.client.lock('KeyL', 1))
        self.assertTrue(self.client.unlock('KeyL'))
        self.assertTrue(self.client.lock('KeyL', 1))
        
    
    def test_SetGet2(self):
        temp = 'test' * 1024 * 1024
        self.client.set('KeyH', temp, 1)
        self.assertEqual(self.client.get('KeyH'), temp)
        
    
    def test_SetGet3(self):
        data = dict()
        for N in range(1024):
            key = 'Key'+str(N)
            data[key] = 'Val'+str(N)
            self.client.set(key, data[key], 3)
        time.sleep(1)
        for N in range(1024):
            key = 'Key'+str(N)
            self.assertEqual(self.client.get(key), data[key])
    
    
    def test_MSetMGet(self):
        self.client.set('MKey#A#A', 'OK', 1)
        self.client.set('MKey#B#B', 'OK', 1)
        self.client.set('MKey#A#C', 'OK', 1)
        self.client.set('MKey#B#D', 'OK', 1)
        self.assertEqual(self.client.mset('MKey#', 'OK!'), 4)
        self.assertEqual(self.client.get('MKey#A#A'), 'OK!')
        self.assertEqual(self.client.get('MKey#B#B'), 'OK!')
        self.assertEqual(self.client.get('MKey#A#C'), 'OK!')
        self.assertEqual(self.client.get('MKey#B#D'), 'OK!')
        self.assertDictEqual(self.client.mget('MKey#'),
            {'MKey#A#A':'OK!','MKey#B#B':'OK!','MKey#A#C':'OK!','MKey#B#D':'OK!'})
        self.assertEqual(self.client.mset('MKey#A#', 'OK!A'), 2)
        self.assertEqual(self.client.mset('MKey#B#', 'OK!B'), 2)
        self.assertDictEqual(self.client.mget('MKey#'),
            {'MKey#A#A':'OK!A','MKey#B#B':'OK!B','MKey#A#C':'OK!A','MKey#B#D':'OK!B'})
        
    
    def test_MTTL(self):
        self.client.set('MKey#A#A', 'OK', 3)
        self.client.set('MKey#B#B', 'OK', 3)
        self.client.set('MKey#A#C', 'OK', 3)
        self.client.set('MKey#B#D', 'OK', 3)
        self.assertDictEqual(self.client.mget('MKey#'),
            {'MKey#A#A':'OK','MKey#B#B':'OK','MKey#A#C':'OK','MKey#B#D':'OK'})
        self.assertEqual(self.client.mttl('MKey#A#', 1), 2)
        time.sleep(1)
        self.assertDictEqual(self.client.mget('MKey#'),{'MKey#B#B':'OK','MKey#B#D':'OK'})


    def test_MDel(self):
        self.client.set('DKey#A#A', 'OK', 3)
        self.client.set('DKey#B#B', 'OK', 3)
        self.client.set('DKey#A#C', 'OK', 3)
        self.client.set('DKey#B#D', 'OK', 3)
        self.assertDictEqual(self.client.mget('DKey#'),
            {'DKey#A#A':'OK','DKey#B#B':'OK','DKey#A#C':'OK','DKey#B#D':'OK'})
        self.assertEqual(self.client.mdelete('DKey#A#'), 2)
        self.assertDictEqual(self.client.mget('DKey#'),{'DKey#B#B':'OK','DKey#B#D':'OK'})
        self.assertEqual(self.client.mdelete('DKey#B#'), 2)
        self.assertDictEqual(self.client.mget('DKey#'),{})
        
    
    def tearDown(self):
        self.client.close()


if __name__ == '__main__':
    unittest.main()