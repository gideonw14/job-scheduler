import random
from operator import attrgetter
import time

JOBS = 100000
SIZE_MAX = 1000
SIZE_MIN = 1
START_MAX = 200
SEED = 15
JOB_SPAWN_RATE = 60
RUNTIME = 3 # in seconds

class Job:
    def __init__(self, size, start):
        self.size = size
        self.completed = 0
        self.start = start

    def __str__(self):
        return "{} / {} complete, start {}".format(self.completed, self.size, self.start)


def create_jobs_random():
    jobs = list()
    for i in range(0, JOBS):
        jobs.append(Job(random.randint(SIZE_MIN, SIZE_MAX), random.randint(0, START_MAX)))
    # print(jobs[i])
    return jobs

def create_jobs_seeded():
    random.seed(SEED)
    return create_jobs_random()

def fifo_scheduler_seeded():
    jobs = create_jobs_seeded()
    jobs_complete = 0
    end_time = time.clock() + RUNTIME
    start = time.clock()
    while (time.clock() < end_time):
        if jobs:
            if jobs[0].completed < jobs[0].size:
                jobs[0].completed += 1
                if jobs[0].completed == jobs[0].size:
                    jobs.pop(0)
                    jobs_complete += 1

    stop = time.clock()
    # for job in jobs:
    # 	print(job)
    speed = stop - start
    print('[FIFO Seeded] Runtime: {} sec. -- Jobs Complete: {}'.format(round(speed, 8), jobs_complete))

def fifo_scheduler_random():
    jobs = list()
    jobs_complete = 0
    start = time.clock()
    end_time = time.clock() + RUNTIME
    while(time.clock() < end_time):
        if random.randint(0, JOB_SPAWN_RATE) == 0:
            jobs.append(Job(random.randint(SIZE_MIN, SIZE_MAX), time.clock()))
        if jobs:
            if jobs[0].completed < jobs[0].size:
                jobs[0].completed += 1
                if jobs[0].completed == jobs[0].size:
                    jobs.pop(0)
                    jobs_complete += 1

    stop = time.clock()
    # for job in jobs:
    # 	print(job)
    speed = stop - start
    print('[FIFO Random] Runtime: {} sec. -- Jobs Complete: {}'.format(round(speed, 8), jobs_complete))

if __name__ == '__main__':
    for i in range(0, 5):
        fifo_scheduler_seeded()
        fifo_scheduler_random()