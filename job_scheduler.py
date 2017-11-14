"""
Team: Gideon Walker and Jordan Kubot
Assignment: Operating Systems Final Project
Date: November 27th, 2017
Description: Job Scheduling
- Implementing the following algorithms
    + First In First Out (FIFO)
    + Earliest Deadline First (EDF)
    + Shortest Time Remaining [First] (STR)
- Jobs can be created from a seed and be predefined for these algorithms or created
  randomly on the fly. That is the meaning of the seeded vs. random functions
"""

import random
from operator import attrgetter
import time
from copy import copy, deepcopy

# Number of jobs to be completed by the seeded operations
JOBS = 100

# How far out the deadline of a job is set
DEADLINE = 200

# Maximum amount of work needed to complete a job
SIZE_MAX = 50

# Minimum amount of work needed to complete a job
SIZE_MIN = 1

# Maximum starting time of a job
START_MAX = 100

# Minimum starting time of a job
START_MIN = 0

# Seed number for seeded operations
SEED = 3

# The spawn rate is 1 / JOB_SPAWN_RATE
JOB_SPAWN_RATE = 20

# How many "clock cycles" the random operation runs for.
RUNTIME = 1000

# See print statements if true
DEBUG = False

# A Job is a work request in our scenario.
# Attributes:
# - size: the amount of time needed to complete the job
# - completed: how much time has been spent on the job
# - start: the time a job instance can be started
# - deadline: the time a job instance needs to be complete by
# - id: the integer identifier of a job


class Job:
    def __init__(self, size, start, deadline, name=None, complete=0):
        self.size = size
        self.completed = complete
        self.start = start
        self.deadline = deadline
        self.id = name

    # This function returns how much time is left for the job to be complete
    def get_remaining_time(self):
        return self.size - self.completed

    def __str__(self):
        return "{}: {} / {} complete, start {}, deadline {}".format(self.id, self.completed, self.size, self.start,
                                                                    self.deadline)

    def __repr__(self):
        return self.__str__()


# Returns a new job with a random start, size, and deadline unless specified by arguments
def create_job(name, start=None, fixed_deadline=False):
    if start:
        start = start
    else:
        start = random.randint(START_MIN, START_MAX)
    size = random.randint(SIZE_MIN, SIZE_MAX)
    if fixed_deadline:
        deadline = size + start + DEADLINE
    else:
        deadline = random.randint(size + start, size + start + DEADLINE)
    return Job(size, start, deadline, name)


# Returns a list of Jobs, uses create_job function in a loop
def create_jobs_random():
    job_list = list()
    for i in range(0, JOBS):
        job = create_job(i, fixed_deadline=True)
        job_list.append(job)
        # print(jobs[i])
    return job_list


# Uses a seed to get the same jobs every time
def create_jobs_seeded():
    random.seed(SEED)
    return create_jobs_random()


def fifo_scheduler_seeded(jobs=None, edf=False):
    # The jobs argument is necessary to compare different scheduling algorithms with the same list of jobs.
    if jobs:
        jobs = jobs
    else:
        jobs = create_jobs_seeded()
    jobs_complete = 0

    # The sorting of the jobs is dependant on what scheduler type we are running
    if edf:
        jobs = sorted(jobs, key=attrgetter('deadline'))
    else:
        jobs = sorted(jobs, key=attrgetter('start'))
    if DEBUG:
        for job in jobs:
            print(job)
    start = time.clock()

    # The clock_cycle keeps track of what loop iteration we are on
    clock_cycle = copy(START_MIN)

    # While we have items in the jobs list
    while jobs:
        # If the deadline of our current job has passed, remove it from the list
        if jobs[0].deadline <= clock_cycle:
            if DEBUG: print('Failed job {}'.format(jobs[0].id))
            jobs.pop(0)

        # If the current job can be started and the job is incomplete, work on the job
        elif jobs[0].start <= clock_cycle and jobs[0].completed < jobs[0].size:
            jobs[0].completed += 1
            # If the job is complete, remove it from the job list
            if jobs[0].completed == jobs[0].size:
                jobs.pop(0)
                jobs_complete += 1
        # Else, we will work on the first available job.
        else:
            soonest_job = min(jobs, key=lambda job: job.start)
            if soonest_job.start <= clock_cycle:
                soonest_job.completed += 1
                if soonest_job.completed == soonest_job.size:
                    jobs.remove(soonest_job)
                    jobs_complete += 1

        # One cycle complete, increment the counter
        clock_cycle += 1
    # End While

    stop = time.clock()
    # Calculate the real time to complete
    speed = stop - start

    # Print the results
    if DEBUG:
        output = 'Seeded] Runtime: {} sec. -- "Clock Cycles": {} -- Jobs Complete: {}'.format(round(speed, 8), clock_cycle,
                                                                                              jobs_complete)
        if edf:
            output = '[EDF ' + output
        else:
            output = '[FIFO ' + output
        print(output)
        return 'Debugging'
    else:
        return (round(speed, 8), clock_cycle, jobs_complete)


# Implementing the Shortest Time Remaining algorithm
def str_scheduler_seeded(jobs=None):
    # The jobs argument is necessary to compare different scheduling algorithms with the same list of jobs.
    if jobs:
        jobs = jobs
    else:
        jobs = create_jobs_seeded()
    jobs_complete = 0

    # The jobs are sorted based on remaining time and secondly by the start time
    jobs = sorted(jobs, key=lambda job: (job.get_remaining_time(), job.start))
    if DEBUG:
        for job in jobs:
            print(job)

    start = time.clock()
    # The clock_cycle keeps track of what loop iteration we are on
    clock_cycle = copy(START_MIN)
    # Current job keeps track of the job we are currently working on. Necessary for preemption
    current_job = None

    # While there are jobs in the job list or we have a current job
    while jobs or current_job:

        # If we have a jobs list still
        if jobs:
            # If we dont have a current job
            if not current_job:
                # If the first job in the list can be started, pop the first job and put it in current job
                if jobs[0].start <= clock_cycle:
                    current_job = jobs.pop(0)
                # Else, make the current job the soonest start time
                else:
                    current_job = min(jobs, key=lambda job: job.start)
                    jobs.remove(current_job)

            # If we do have a current job, attempt to preempt this job
            else:
                # If the first job in the list has less remaining time than the current job and
                # the first job can be started, put the current job back on the jobs list and
                # pop the first job off the list and put it in current job.
                if jobs[0].get_remaining_time() < current_job.get_remaining_time() and jobs[0].start <= clock_cycle:
                    if DEBUG:
                        print('Pre-empting job {} with job {} at time {}'.format(current_job, jobs[0], clock_cycle))
                    jobs.append(current_job)
                    current_job = jobs.pop(0)
                    # The job list is re-sorted by shortest remaining time
                    jobs = sorted(jobs, key=lambda job: (job.get_remaining_time(), job.start))

        # If the deadline has passed, delete the job
        if current_job.deadline <= clock_cycle:
            if DEBUG: print('Failed job {}'.format(current_job.id))
            current_job = None

        # If we can start the current job, work on it
        elif current_job.start <= clock_cycle:
            current_job.completed += 1
            # If the job is complete now, record it and move on
            if current_job.completed == current_job.size:
                current_job = None
                jobs_complete += 1

        # One cycle complete, increment the counter
        clock_cycle += 1
    # End While

    stop = time.clock()
    # Calculate the real time it took to run the process
    speed = stop - start

    # Print the output
    if DEBUG:
        print(
            '[STR Seeded] Runtime: {} sec. -- "Clock Cycles": {} -- Jobs Complete: {}'.format(round(speed, 8),
                                                                                              clock_cycle,
                                                                                              jobs_complete))
        return 'Debugging'
    else:
        return (round(speed, 8), clock_cycle, jobs_complete)

# This function implements all three scheduling algorithms based on the flags passed to it
# This function generates jobs randomly on the fly, so it will always produce different results
def fifo_scheduler_random(edf=False, str=False):
    jobs = list()
    job_number = 0
    jobs_complete = 0
    start = time.clock()
    clock_cycle = copy(START_MIN)
    current_job = None

    # While we are still in runtime
    while(clock_cycle < RUNTIME):
        # Decide if we should spawn a job
        if random.randint(0, JOB_SPAWN_RATE) == 0:
            job = create_job(job_number, start=clock_cycle, fixed_deadline=True)
            job_number += 1
            jobs.append(job)

            # Sort the jobs based on which algorithm we are using.
            # EDF is sorted by deadline, STR is sorted by time remaining, and FIFO by start time
            if edf:
                jobs = sorted(jobs, key=lambda job: (job.deadline, job.start))
            elif str:
                jobs = sorted(jobs, key=lambda job: (job.get_remaining_time(), job.start))
            else:
                jobs = sorted(jobs, key=attrgetter('start'))
            if DEBUG:
                for job in jobs:
                    print(job)

        if jobs:
            # If we do not have a current job, grab the first job.
            # The sorting from previously ensures this is the correct job
            if not current_job:
               current_job = jobs.pop(0)

            # STR will pre-empt the current job if a shorter job comes up
            if jobs:
                if str and jobs[0].get_remaining_time() < current_job.get_remaining_time():
                    jobs.append(current_job)
                    current_job = jobs.pop(0)
                    jobs = sorted(jobs, key=lambda job: (job.get_remaining_time(), job.start))

            # If the deadline has passed for the current job, delete it.
            if current_job.deadline <= clock_cycle:
                if DEBUG: print('Failed job {} at time {}'.format(current_job.id, clock_cycle))
                if jobs:
                    current_job = jobs.pop(0)
                else:
                    current_job = None

            # If the current job is incomplete, work on it
            elif current_job.completed < current_job.size:
                current_job.completed += 1
                # If the current job is now complete, record it and move on to the next job
                if current_job.completed == current_job.size:
                    if jobs:
                        current_job = jobs.pop(0)
                    else:
                        current_job = None
                    jobs_complete += 1

        # Onc cycle complete, increment the counter
        clock_cycle += 1
    # End While

    stop = time.clock()
    if DEBUG:
        for job in jobs:
            print(job)

    # Calculate the real time and print the output
    speed = stop - start
    if DEBUG:
        output = 'Random] Runtime: {} sec. -- "Clock Cycles": {} -- Jobs Complete: {}'.format(round(speed, 8), clock_cycle,
                                                                                       jobs_complete)
        if str:
            output = '[STR ' + output
        elif edf:
            output = '[EDF ' + output
        else:
            output = '[FIFO ' + output
        print(output)
        return 'Debugging'
    else:
        return (round(speed, 8), clock_cycle, jobs_complete)


# Running the different algorithms in the main function
if __name__ == '__main__':
    seeded = True
    loops = 10
    if seeded:
        output_file = open('job_scheduler_seeded.csv', 'w')
        output_file.write('Jobs:, {}\n'.format(JOBS))
        output_file.write('Size:, {}, {}\n'.format(SIZE_MIN, SIZE_MAX))
        output_file.write('Deadline:, {}\n'.format(DEADLINE))
        output_file.write(',FIFO, ED, STR\n')
        for i in range(0, loops):
            SEED = i
            jobs = create_jobs_seeded()
            # Deep copy ensures we are passing the jobs by value and not reference
            # (we want all the algorithms to use the exact same job list)
            fifo_output = fifo_scheduler_seeded(deepcopy(jobs))
            edf_output = fifo_scheduler_seeded(deepcopy(jobs), edf=True)
            str_output = str_scheduler_seeded(deepcopy(jobs))
            output_file.write('{}, {}, {}, {}\n'.format(SEED, fifo_output[2], edf_output[2], str_output[2]))
    else:
        output_file = open('job_scheduler_random.csv', 'w')
        output_file.write('Runtime:, {}\n'.format(RUNTIME))
        output_file.write('Size:, {}, {}\n'.format(SIZE_MIN, SIZE_MAX))
        output_file.write('Deadline:, {}\n'.format(DEADLINE))
        output_file.write('Spawn:, {}\n'.format(JOB_SPAWN_RATE))
        output_file.write(',FIFO, ED, STR\n')
        for i in range(0, loops):
            fifo_output = fifo_scheduler_random()
            edf_output = fifo_scheduler_random(edf=True)
            str_output = fifo_scheduler_random(str=True)
            output_file.write('{}, {}, {}, {}\n'.format(i, fifo_output[2], edf_output[2], str_output[2]))