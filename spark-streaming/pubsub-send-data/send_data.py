import random
from boto import kinesis
from google.cloud import pubsub
import sys
import os

# connecting to Kinesis stream
#region = 'us-east-2'
#kinesisStreamName = 'MyDataStream'
#kinesis = kinesis.connect_to_region(region)

# считывание данных и отправка в kinesis
def read_file(filename, topic_url, publisher):
    with open(filename, "rb") as file_csv:
        for line in file_csv.readlines():
            #patitionKey = random.choice('abcdefghij')
            #result = kinesis.put_record(kinesisStreamName, line, patitionKey)
            publisher.publish(topic_url, line)
            print(line)


if __name__ == "__main__":
    # os.environ['PUBSUB_EMULATOR_HOST'] = 'localhost:8085'
    # os.environ['PUBSUB_PROJECT_ID'] = 'u-123'
    # publisher = pubsub.PublisherClient()
    # topic = 'projects/u-123/topics/laaw'
    #publisher.get_topic(topic)
    if len(sys.argv) != 2:
        print("""Error: Incorrect number of parameters.
        Usage: python send_data.py <project>
            - project: ID of your GCP project
    """)
        sys.exit()
    PROJECT = sys.argv[1]
    publisher = pubsub.PublisherClient()

    topic_url = 'projects/{project_id}/topics/{topic}'.format(
        project_id=PROJECT,
        topic="kicks",
    )
    topic_url = 'projects/my-spark-project-270614/topics/kicks'
    #topic_path = publisher.topic_path()
    #topic = publisher.create_topic(topic_url)#, subscription="project-sub")
    filename = r"C:\Users\Rabbit\Desktop\kickstarter-projects\test.csv"
    #filename = r"C:\Users\Rabbit\Desktop\kickstarter-projects\ks-projects-201612.csv"
    read_file(filename, topic_url, publisher)
    #filename = r"C:\Users\Rabbit\Desktop\kickstarter-projects\ks-projects-201801.csv"
    #read_file(filename, topic_url, publisher)
