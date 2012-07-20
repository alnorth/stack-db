import dateutil.parser
from pymongo import Connection
import Queue
import sys
import threading
import time
from xml.sax import make_parser, handler

# Set up the database connection
connection = Connection()
db = connection.stackdb
questions = db.questions

questions.ensure_index("question_id")

# For now we clear the collection before we start
questions.remove()

def import_question(questions, question_id, title, body, tags, last_activity_date, score, answer_count, accepted_answer_id):
    question = questions.find_one({"question_id": question_id})
    exists = True
    if not question:
        question = {
            "question_id": question_id,
            "answers": []
        }
        exists = False
    question["title"] = title
    question["body"] = body
    question["tags"] = tags
    question["last_activity_date"] = last_activity_date
    question["score"] = score
    question["accepted_answer_id"] = accepted_answer_id

    if exists:
        questions.update({"question_id": question_id}, question)
    else:
        questions.insert(question)

def import_answer(questions, question_id, answer_id, body, last_activity_date, score):
    answer = None
    question = questions.find_one({"question_id": question_id})
    question_exists = False
    if question:
        for a in question["answers"]:
            if a["answer_id"] == answer_id:
                answer = a
        question_exists = True
        if not answer:
            answer = {"answer_id": answer_id}
            question["answers"].append(answer)
    else:
        answer = {"answer_id": answer_id}
        question = {
            "question_id": question_id,
            "answers": [answer]
        }
    answer["body"] = body
    answer["last_activity_date"] = last_activity_date
    answer["score"] = score

    if question_exists:
        questions.update({"question_id": question_id}, question)
    else:
        questions.insert(question)


class ThreadPosts(threading.Thread):

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            attrs = self.queue.get()

            global questions
            if attrs["PostTypeId"] == "1":
                import_question(
                    questions,
                    int(attrs["Id"]),
                    attrs["Title"],
                    attrs["Body"],
                    attrs["Tags"].lstrip("<").rstrip(">").split("><"),
                    dateutil.parser.parse(attrs["LastActivityDate"]),
                    int(attrs["Score"]),
                    int(attrs["AnswerCount"]) if "AnswerCount" in attrs else 0,
                    int(attrs["AcceptedAnswerId"]) if "AcceptedAnswerId" in attrs else 0
                )
            if attrs["PostTypeId"] == "2":
                import_answer(
                    questions,
                    int(attrs["ParentId"]),
                    int(attrs["Id"]),
                    attrs["Body"],
                    dateutil.parser.parse(attrs["LastActivityDate"]),
                    int(attrs["Score"])
                )

class ThreadQueueStatus(threading.Thread):

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            print "Queue size - %i" % self.queue.qsize()
            time.sleep(30)

class SOProcessor(handler.ContentHandler):

    def __init__(self, queue):
        handler.ContentHandler.__init__(self)
        self.queue = queue

    def startElement(self, name, attrs):
        if name == "row":
            self.queue.put(attrs)

queue = Queue.Queue(maxsize=5000)

# Start 5 threads to process questions once they're retrieved from the XML
for i in range(5):
    t = ThreadPosts(queue)
    t.setDaemon(True)
    t.start()

status = ThreadQueueStatus(queue)
status.setDaemon(True)
status.start()

parser = make_parser()
parser.setContentHandler(SOProcessor(queue))
parser.parse(open(sys.argv[1]))

queue.join()
