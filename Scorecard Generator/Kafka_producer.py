import threading
import time
import random
from kafka import KafkaProducer

exitFlag = 0
delay_list = [0.01, 0.02, 0.03]

class myProducerThread (threading.Thread):
   def __init__(self, threadID, name, counter):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
      self.counter = counter
   def run(self):
      prod = KafkaProducer(bootstrap_servers = ['localhost:9092'])
      with open('194161007-'+self.name+'-commentary.txt', 'r') as ip_file:
         while(True):
            ip_str = ip_file.readline()
            if (ip_str == ''):
               break
            ip_str_parts = ip_str.split('~')
            prod.send(ip_str_parts[0], value=bytes(ip_str_parts[2], encoding='utf-8'))
            prod.flush()
            delay_value = random.choice(delay_list)
            print("Match:", ip_str_parts[0], "Sleep:",delay_value)
            time.sleep(delay_value)
      prod.send(self.name, value=bytes('File End!!!', encoding='utf-8'))
      print(self.name, "File Over")
      prod.flush()

# Create new threads
thread_names = {
                1:'4143', 2:'4144', 3:'4145',4:'4146',5:'4147',6:'4148',
                7:'4149',8:'4150',9:'4151',10:'4152',11:'4152a', 12:'4153',
                13:'4154',14:'4155',15:'4156',16:'4156a', 17:'4157', 18:'4157a',
                19:'4158',20:'4159', 21:'4160', 22:'4161', 23:'4162', 24:'4163',
                25:'4165', 26:'4166', 27:'4168', 28:'4169', 29:'4170', 30:'4171',
                31:'4172', 32:'4173', 33:'4174', 34:'4175', 35:'4176', 36:'4177',
                37:'4178', 38:'4179', 39:'4180', 40:'4182', 41:'4183', 42:'4184',
                43:'4186', 44:'4187', 45:'4188', 46:'4190', 47:'4191', 48:'4192'
                }

threads = []
for t_n in thread_names.keys():
   thrd = myProducerThread(t_n, thread_names[t_n],t_n)
   threads.append(thrd)

#thread1 = myThread(1, "testing", 1)
#thread2 = myThread(2, "testing2", 2)

for t in threads:
   t.start()

for t in threads:
   t.join()

# Start new Threads
#thread1.start()
#thread2.start()
#thread1.join()
#thread2.join()



print ("Exiting Main Thread")

