#EXAMPLE SOURCE - working on single node ...fuck yeah


import logging
import uuid
import time
import sys

from mesos.interface import Scheduler
from mesos.native import MesosSchedulerDriver
from mesos.interface import mesos_pb2

logging.basicConfig(level=logging.INFO)

def new_task(offer):
    task = mesos_pb2.TaskInfo()
    id = uuid.uuid4()
    task.task_id.value = str(id)
    task.slave_id.value = offer.slave_id.value
    task.name = "task {}".format(str(id))

    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = 1

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = 1

    return task


class HelloWorldScheduler(Scheduler):
    def __init__(self):
        self.numbers = [x.strip() for x in  sys.argv[3].split(',')] #[10, 5, 3];
        self.counter = 0
        self.finished_tasks = 0
        self.master_ip = sys.argv[1]
        self.usecase = sys.argv[2]

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: {}".format(framework_id))

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: {}".format([o.id.value for o in offers]))
        # if an offer is given, take it and use it for a task
        # triggering a python task that computes fibonacci number
        for offer in offers:
            if self.counter < len(self.numbers):
                task = new_task(offer)
                task.command.value = "python "+ self.usecase +" {ip} {params}".format(ip=self.master_ip, params=self.numbers[self.counter])
                time.sleep(2)
                logging.info("Launching task {task} "
                             "using offer {offer}.".format(task=task.task_id.value,
                                                         offer=offer.id.value))
                tasks = [task]
                driver.launchTasks(offer.id, tasks)
                self.counter += 1
            else:
                print "All tasks done, stopping framework"
                driver.stop()

if __name__ == '__main__':
    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" 
    framework.name = "hello-world"
    driver = MesosSchedulerDriver(
        HelloWorldScheduler(),
        framework,
        "zk://localhost:2181/mesos"  
    )
    driver.run()
	
	
	


#now the executor py  named slave_task.py

import sys
import resource

number_array = sys.argv[2]

def fibRec(n):
    if n == 0: return 0
    elif n == 1: return 1
    else: return fibRec(n-1)+fibRec(n-2)
	
fibres = fibRec(number_array)

fo = open("foo"+str(fibres)+".txt", "wb")
fo.write("Computing fibonacci number of "+str(number_array)+"\n")
fo.write("The result is: "+str(fibres)+"\n")
fo.write("Current memory consumption: "+str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1000))
fo.close()



START WITH: python mesos_fib.py 10.155.208.23 /slave_task.py "10,5,3"

