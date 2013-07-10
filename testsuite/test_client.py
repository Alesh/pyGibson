import time
import unittest
import logging
import gibson

servers = ["/tmp/gibson1.sock",
           "/tmp/gibson2.sock",
           "/tmp/gibson3.sock",
           "/tmp/gibson4.sock"
           ]

class test_Client(unittest.TestCase):
    
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        self.client = gibson.Client(servers, debug=True)
    
    #def test_Connected(self):
    #    self.assertEqual(self.client.connected, True)
    #
    #def test_SetGet(self):
    #    self.client.set('Key', 'OK', 1)
    #    self.assertEqual(self.client.get('Key'), 'OK')
    #
    #def test_SetGetDelGet(self):
    #    self.client.set('KeyA', 'OK', 1)
    #    self.assertEqual(self.client.get('KeyA'), 'OK')
    #    self.assertEqual(self.client.delete('KeyA'), True)
    #    self.assertEqual(self.client.get('KeyA'), None)
    #
    #def test_SetIncDec(self):
    #    self.client.set('KeyN', 0, 1)
    #    self.assertEqual(self.client.inc('KeyN'), 1)
    #    self.assertEqual(self.client.inc('KeyN'), 2)
    #    self.assertEqual(self.client.inc('KeyN'), 3)
    #    self.assertEqual(self.client.get('KeyN'), 3)
    #    self.assertEqual(self.client.dec('KeyN'), 2)
    #    self.assertEqual(self.client.dec('KeyN'), 1)
    #    self.assertEqual(self.client.dec('KeyN'), 0)
    #    self.assertEqual(self.client.get('KeyN'), 0)
    #    self.assertEqual(self.client.dec('KeyN'), -1)
    #    self.assertEqual(self.client.dec('KeyN'), -2)
    #    self.assertEqual(self.client.get('KeyN'), -2)
    #    
    #def test_TTL(self):
    #    self.client.set('KeyT', 'OK', 2)
    #    time.sleep(1)
    #    self.assertEqual(self.client.get('KeyT'), 'OK')
    #    time.sleep(1)
    #    self.assertEqual(self.client.get('KeyT'), None)
    #
    #def test_TTL2(self):
    #    self.client.set('KeyT2', 'OK', 2)
    #    time.sleep(1)
    #    self.assertEqual(self.client.get('KeyT2'), 'OK')
    #    self.assertEqual(self.client.ttl('KeyT2', 2), True)
    #    time.sleep(1)
    #    self.assertEqual(self.client.get('KeyT2'), 'OK')
    #    
    #
    #def test_LockUnlock(self):
    #    self.client.set('KeyL', 'OK', 3)
    #    self.assertEqual(self.client.get('KeyL'), 'OK')
    #    self.assertTrue(self.client.lock('KeyL', 1))
    #    self.assertFalse(self.client.lock('KeyL', 1))
    #    time.sleep(1)
    #    self.assertTrue(self.client.lock('KeyL', 1))
    #    time.sleep(1)
    #    self.assertEqual(self.client.get('KeyL'), 'OK')
    #    self.assertTrue(self.client.lock('KeyL', 1))
    #    self.assertTrue(self.client.unlock('KeyL'))
    #    self.assertTrue(self.client.lock('KeyL', 1))
    #    
    #
    #def test_SetGet2(self):
    #    temp = 'test' * 1024 * 1024
    #    self.client.set('KeyH', temp, 1)
    #    self.assertEqual(self.client.get('KeyH'), temp)
    #    
    #
    #def test_SetGet3(self):
    #    data = dict()
    #    for N in range(1024):
    #        key = 'Key'+str(N)
    #        data[key] = 'Val'+str(N)
    #        self.client.set(key, data[key], 3)
    #    time.sleep(1)
    #    for N in range(1024):
    #        key = 'Key'+str(N)
    #        self.assertEqual(self.client.get(key), data[key])
    #
    #
    #def test_MSetMGet(self):
    #    self.client.set('MKey#A#A', 'OK', 1)
    #    self.client.set('MKey#B#B', 'OK', 1)
    #    self.client.set('MKey#A#C', 'OK', 1)
    #    self.client.set('MKey#B#D', 'OK', 1)
    #    self.assertEqual(self.client.mset('MKey#', 'OK!'), 4)
    #    self.assertEqual(self.client.get('MKey#A#A'), 'OK!')
    #    self.assertEqual(self.client.get('MKey#B#B'), 'OK!')
    #    self.assertEqual(self.client.get('MKey#A#C'), 'OK!')
    #    self.assertEqual(self.client.get('MKey#B#D'), 'OK!')
    #    self.assertDictEqual(self.client.mget('MKey#'),
    #        {'MKey#A#A':'OK!','MKey#B#B':'OK!','MKey#A#C':'OK!','MKey#B#D':'OK!'})
    #    self.assertEqual(self.client.mset('MKey#A#', 'OK!A'), 2)
    #    self.assertEqual(self.client.mset('MKey#B#', 'OK!B'), 2)
    #    self.assertDictEqual(self.client.mget('MKey#'),
    #        {'MKey#A#A':'OK!A','MKey#B#B':'OK!B','MKey#A#C':'OK!A','MKey#B#D':'OK!B'})
    #    
    #
    #def test_MTTL(self):
    #    self.client.set('MKey#A#A', 'OK', 3)
    #    self.client.set('MKey#B#B', 'OK', 3)
    #    self.client.set('MKey#A#C', 'OK', 3)
    #    self.client.set('MKey#B#D', 'OK', 3)
    #    self.assertDictEqual(self.client.mget('MKey#'),
    #        {'MKey#A#A':'OK','MKey#B#B':'OK','MKey#A#C':'OK','MKey#B#D':'OK'})
    #    self.assertEqual(self.client.mttl('MKey#A#', 1), 2)
    #    time.sleep(1)
    #    self.assertDictEqual(self.client.mget('MKey#'),{'MKey#B#B':'OK','MKey#B#D':'OK'})
    #
    #
    #def test_MDel(self):
    #    self.client.set('DKey#A#A', 'OK', 3)
    #    self.client.set('DKey#B#B', 'OK', 3)
    #    self.client.set('DKey#A#C', 'OK', 3)
    #    self.client.set('DKey#B#D', 'OK', 3)
    #    self.assertDictEqual(self.client.mget('DKey#'),
    #        {'DKey#A#A':'OK','DKey#B#B':'OK','DKey#A#C':'OK','DKey#B#D':'OK'})
    #    self.assertEqual(self.client.mdelete('DKey#A#'), 2)
    #    self.assertDictEqual(self.client.mget('DKey#'),{'DKey#B#B':'OK','DKey#B#D':'OK'})
    #    self.assertEqual(self.client.mdelete('DKey#B#'), 2)
    #    self.assertDictEqual(self.client.mget('DKey#'),{})
    
    #def test_MIncDec(self):
    #    self.client.set('MIKey#A#A', 1, 3)
    #    self.client.set('MIKey#B#B', 2, 3)
    #    self.client.set('MIKey#A#C', 3, 3)
    #    self.client.set('MIKey#B#D', 4, 3)
    #    self.assertEqual(self.client.minc('MIKey#'), 4)
    #    self.assertEqual(self.client.minc('MIKey#A'), 2)
    #    self.assertDictEqual(self.client.mget('MIKey#'), 
    #        {'MIKey#A#A':3,'MIKey#B#B':3,'MIKey#A#C':5,'MIKey#B#D':5})
    #    self.assertEqual(self.client.mdec('MIKey#'), 4)
    #    self.assertEqual(self.client.mdec('MIKey#'), 4)
    #    self.assertEqual(self.client.mdec('MIKey#'), 4)
    #    self.assertEqual(self.client.mdec('MIKey#B'), 2)
    #    self.assertEqual(self.client.mdec('MIKey#B'), 2)
    #    self.assertEqual(self.client.mdec('MIKey#B'), 2)
    #    self.assertDictEqual(self.client.mget('MIKey#'), 
    #        {'MIKey#A#A':0,'MIKey#B#B':-3,'MIKey#A#C':2,'MIKey#B#D':-1})

    #def test_MLockUnlock(self):
    #    self.client.set('MLKey#A#A', 1, 3)
    #    self.client.set('MLKey#B#B', 2, 3)
    #    self.client.set('MLKey#A#C', 3, 3)
    #    self.client.set('MLKey#B#D', 4, 3)        
    #    self.assertEqual(self.client.mlock('MLKey', 1), 4)
    #    self.assertEqual(self.client.mlock('MLKey', 1), 0)
    #    time.sleep(1)
    #    self.assertEqual(self.client.mlock('MLKey', 1), 4)
    #    self.assertEqual(self.client.munlock('MLKey'), 4)
    #    self.assertEqual(self.client.mlock('MLKey', 1), 4)
    #    self.assertEqual(self.client.munlock('MLKey#A'), 2)
    #    self.assertEqual(self.client.munlock('MLKey#C'), 0)
        

    #def test_MCount(self):
    #    self.client.set('MCKey#A#A', 1, 3)
    #    self.client.set('MCKey#B#B', 2, 3)
    #    self.client.set('MCKey#A#C', 3, 3)
    #    self.client.set('MCKey#B#D', 4, 3)
    #    self.assertEqual(self.client.count('MCKey'), 4)
    #    self.assertEqual(self.client.mdelete('MCKey#B'), 2)
    #    self.assertEqual(self.client.count('MCKey'), 2)
    #    self.assertEqual(self.client.mdelete('MCKey#A'), 2)
    #    self.assertEqual(self.client.count('MCKey'), 0)
    
    def test_SizeOf(self):
        value = 'X'*1024
        self.client.set('SOKey', value, 3)
        self.assertEqual(self.client.sizeOf('SOKey'), 1024)
        #value *= 1024
        #self.client.set('SOKey', value, 3)
        #self.assertEqual(self.client.sizeOf('SOKey'), 1024*1024)

    def test_MSizeOf(self):
        value = 'X'*1024
        self.client.set('SOKey#A', value, 3)
        self.client.set('SOKey#B', value, 3)
        self.assertEqual(self.client.sizeOf('SOKey#A'), 1024)
        self.assertEqual(self.client.sizeOf('SOKey#B'), 1024)
        self.assertEqual(self.client.msizeOf('SOKey#'), 1024*2)
        
        
        
    
    
    def tearDown(self):
        self.client.close()


if __name__ == '__main__':
    unittest.main()