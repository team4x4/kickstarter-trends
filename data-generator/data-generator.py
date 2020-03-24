import sys
import time
from google.cloud import pubsub
from ratelimit import rate_limited

ONE_MINUTE = 60
TOPIC_NAME = 'kicks'

if len(sys.argv) != 5:
    print("""Error: Incorrect number of parameters.
    Usage: python data-generator.py <project> <time> <rate>
        - project: ID of your GCP project
        - time: total execution time, in minutes
        - rate: number of projects data per minute
        - file name: file csv
        """)
    sys.exit()

PROJECT = sys.argv[1]
TOTAL_TIME = int(sys.argv[2])
RATE = int(sys.argv[3])
FILE_NAME = sys.argv[4]

try:
    FILE = open(FILE_NAME)
    FILE.readline()
except IOError:
    print("File not accessible")
    sys.exit()


publisher = pubsub.PublisherClient()
topic_url = 'projects/{project_id}/topics/{topic}'.format(
    project_id=PROJECT,
    topic=TOPIC_NAME,
)
num_projects = 0


@rate_limited(RATE, ONE_MINUTE)
def get_project():
    global num_projects
    project_info = FILE.readline()
    if "" == project_info:
        FILE.seek(0)
        FILE.readline()
    print(project_info, time.time())
    publisher.publish(topic_url, project_info.encode('utf-8'))
    num_projects += 1
    

now = start_time = time.time()
while now < start_time + TOTAL_TIME * ONE_MINUTE:
    get_project()
    now = time.time()


elapsed_time = time.time() - start_time
print("Elapsed time: %s minutes" % (elapsed_time / 60))
print("Number of projects: %s" % num_projects)
FILE.close()
