import time
import gibson.client

servers = ["/home/research/gibson.sock"]
client = gibson.client.Client(servers)

client.set('KeyT', 'OK', 3)
start = time.time()
for N in range(30):
    time.sleep(0.1)
    print time.time()-start, client.get('KeyT')
    
    