import dateutil.parser
from pymongo import Connection
import sys
import time
from xml.sax import make_parser, handler

# Set up the database connection
connection = Connection()
db = connection.stackdb
questions = db.questions

questions.ensure_index("question_id", unique=True)
questions.ensure_index("answers.answer_id", unique=True, sparse=True)

# For now we clear the collection before we start
questions.remove()

class QuestionProcessor(handler.ContentHandler):
    def startElement(self, name, attrs):
        if name == "row":
            if attrs["PostTypeId"] == "1":
                question = {
                    "question_id": int(attrs["Id"]),
                    "title": attrs["Title"],
                    "body": attrs["Body"],
                    "tags": attrs["Tags"].lstrip("<").rstrip(">").split("><"),
                    "last_activity_date": dateutil.parser.parse(attrs["LastActivityDate"]),
                    "score": int(attrs["Score"]),
                    "accepted_answer_id": int(attrs["AcceptedAnswerId"]) if "AcceptedAnswerId" in attrs else 0,
                    "answers": []
                }

                # We assume that the question doesn't exist yet
                questions.insert(question)

            if int(attrs["Id"]) % 5000 == 0:
                print attrs["Id"]

class AnswerProcessor(handler.ContentHandler):
    def startElement(self, name, attrs):
        if name == "row":
            if attrs["PostTypeId"] == "2":
                answer = {
                    "answer_id": int(attrs["Id"]),
                    "body": attrs["Body"],
                    "last_activity_date": dateutil.parser.parse(attrs["LastActivityDate"]),
                    "score": int(attrs["Score"])
                }

                # We assume that the question exists, but not the answer
                result = questions.update({"question_id": int(attrs["ParentId"])}, { "$push": { "answers":  answer}}, safe=True)
                if result["n"] != 1:
                    print "Answer found for non-existent question, q=%s, a=%s" % (attrs["ParentId"], attrs["Id"])

            if int(attrs["Id"]) % 5000 == 0:
                print attrs["Id"]

# In some of the data dumps answers appear before the questions they're answering.
# We deal with this by running over the posts.xml twice. We import questions the
# first time, and then answers.

print "Importing questions ..."
qparser = make_parser()
qparser.setContentHandler(QuestionProcessor())
qparser.parse(open(sys.argv[1]))

print "Importing answers ..."
aparser = make_parser()
aparser.setContentHandler(AnswerProcessor())
aparser.parse(open(sys.argv[1]))
