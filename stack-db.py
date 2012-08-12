from bson.code import Code
from datetime import datetime
import dateutil.parser
import math
import os
import pymongo
import stackexchange
import sys
import time
from xml.sax import make_parser, handler

# Set up the database connection
connection = pymongo.Connection()
db = connection.stackdb

def print_percentage(percentage):
    hashes = int(math.floor(percentage)/2)
    text = "\r[{0}{1}] {2}%".format("#" * hashes, " " * (50 - hashes), round(percentage, 1))
    sys.stdout.write(text)
    sys.stdout.flush()

if len(sys.argv) == 2:
    # Set up a temporary collection for insertion
    tmp_posts = db.tmp_posts
    # Make sure it's empty
    tmp_posts.drop()

    tmp_posts.ensure_index([("question_id", pymongo.ASCENDING), ("post_type", pymongo.ASCENDING)])

    class SOProcessor(handler.ContentHandler):

        def __init__(self, pf):
            handler.ContentHandler.__init__(self)
            self.pf = pf

        def startElement(self, name, attrs):
            if name == "row":
                if attrs["PostTypeId"] == "1":
                    question = {
                        "question_id": int(attrs["Id"]),
                        "post_type": 1,
                        "title": attrs["Title"],
                        "body": attrs["Body"],
                        "tags": attrs["Tags"].lstrip("<").rstrip(">").split("><"),
                        "last_activity_date": dateutil.parser.parse(attrs["LastActivityDate"]),
                        "last_updated_date": dateutil.parser.parse(attrs["LastActivityDate"]),
                        "score": int(attrs["Score"]),
                        "accepted_answer_id": int(attrs["AcceptedAnswerId"]) if "AcceptedAnswerId" in attrs else 0,
                        "answers": []
                    }
                    tmp_posts.insert(question)

                if attrs["PostTypeId"] == "2":
                    answer = {
                        "answer_id": int(attrs["Id"]),
                        "body": attrs["Body"],
                        "last_activity_date": dateutil.parser.parse(attrs["LastActivityDate"]),
                        "score": int(attrs["Score"])
                    }
                    question = {
                        "question_id": int(attrs["ParentId"]),
                        "post_type": 2,
                        "answers": [answer]
                    }
                    tmp_posts.insert(question)

                if int(attrs["Id"]) % 5000 == 0:
                    self.pf.print_progress()

    class PercentageFile(object):

        def __init__(self, filename):
            self.size = os.stat(filename)[6]
            self.delivered = 0
            self.f = file(filename)

        def read(self, size=None):
            if size is None:
                self.delivered = self.size
                return self.f.read()
            data = self.f.read(size)
            self.delivered += len(data)
            return data

        @property
        def percentage(self):
            return float(self.delivered) / self.size * 100.0

        def print_progress(self):
            print_percentage(self.percentage)

    # We insert everything to start with, then use Map/Reduce to bring answers and questions together

    print "Importing posts..."
    qparser = make_parser()
    pf = PercentageFile(sys.argv[1])
    qparser.setContentHandler(SOProcessor(pf))
    qparser.parse(pf)
    print ""

    # Collection for combined questions
    questions = db.questions
    # Make sure it's empty
    questions.drop()

    print "Combining posts..."
    current_question = None
    num_posts = tmp_posts.count()
    current_index = 0
    for post in tmp_posts.find(sort=[("question_id", pymongo.ASCENDING), ("post_type", pymongo.ASCENDING)]):
        if post["post_type"] == 1:
            if current_question:
                # We're moving on to the next question, save the current one.
                # TODO: remove the post_type field
                questions.insert(current_question)
            current_question = post
        else:
            current_question["answers"].append(post["answers"][0])
        current_index += 1
        print_percentage(float(current_index) / num_posts * 100.0)

    # Insert the last one
    questions.insert(current_question)
    print ""

    tmp_posts.drop()

# Now we update the questions in the database.
so = stackexchange.Site(stackexchange.StackOverflow)
so.be_inclusive()
so.impose_throttling = True

questions = db.questions
questions.ensure_index([("question_id", pymongo.ASCENDING)])
questions.ensure_index([("last_activity_date", pymongo.DESCENDING)])

latest_active_question = questions.find_one(sort=[("last_activity_date", pymongo.DESCENDING)])
latest_activity_date = latest_active_question["last_activity_date"]
latest_activity_date_as_unix = int(time.mktime(latest_activity_date.timetuple()))
print "Fetching questions active after %s" % str(latest_activity_date)

rq = so.recent_questions(min=latest_activity_date_as_unix, order="asc", pagesize=100)
index = 0
requests_left_current = 0

for q in rq:
    question = questions.find_one({"question_id": int(q.id)})
    previously_existed = False
    if question:
        previously_existed = True
    else:
        question = {
            "question_id": int(q.id)
        }

    question["title"] = q.title
    question["body"] = q.body
    question["tags"] = q.tags
    question["last_activity_date"] = q.last_activity_date
    question["last_updated_date"] = datetime.utcnow()
    question["score"] = int(q.score)
    if hasattr(q, "accepted_answer_id"):
        question["accepted_answer_id"] = int(q.accepted_answer_id)

    q_answers = []
    for a in q.answers:
        q_a = {
            "answer_id": a.id,
            "body": a.body,
            "last_activity_date": a.last_activity_date,
            "score": a.score
        }
        q_answers.append(q_a)
    question["answers"] = q_answers

    if previously_existed:
        questions.update({"question_id": int(q.id)}, question)
    else:
        questions.insert(question)

    # Print progress every 100 questions
    index += 1
    if index % 100 == 0:
        print_percentage(100 * (q.last_activity_date - latest_activity_date).total_seconds() / (datetime.utcnow() - latest_activity_date).total_seconds())

    # Monitor our request allowance. Pause if we're running out
    if (requests_left_current != so.requests_left) and so.requests_left < 100:
        print "\n%s requests left, pausing" % so.requests_left
        requests_left_current = so.requests_left
        time.sleep(60)

