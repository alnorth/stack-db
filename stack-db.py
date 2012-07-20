import dateutil.parser
from pymongo import Connection
import sys
import time
from xml.sax import make_parser, handler

# Set up the database connection
connection = Connection()
db = connection.stackdb
questions = db.questions

questions.ensure_index("question_id")

# For now we clear the collection before we start
questions.remove()

# This function assumes that the question isn't present already
def insert_question(questions, question_id, title, body, tags, last_activity_date, score, answer_count, accepted_answer_id):
    question = {
        "question_id": question_id,
        "title": title,
        "body": body,
        "tags": tags,
        "last_activity_date": last_activity_date,
        "score": score,
        "accepted_answer_id": accepted_answer_id,
        "answers": []
    }

    questions.insert(question)

# This function assumes that the answer isn't present already
def insert_answer(questions, question_id, answer_id, body, last_activity_date, score):
    answer = {
        "answer_id": answer_id,
        "body": body,
        "last_activity_date": last_activity_date,
        "score": score
    }

    result = questions.update({"question_id": question_id}, { "$push": { "answers":  answer}}, safe=True)
    if result["n"] != 1:
        print "Answer found for non-existent question, q=%i, a=%i" % (question_id, answer_id)


class QuestionProcessor(handler.ContentHandler):
    def startElement(self, name, attrs):
        if name == "row":
            if attrs["PostTypeId"] == "1":
                insert_question(
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

            if int(attrs["Id"]) % 5000 == 0:
                print attrs["Id"]

class AnswerProcessor(handler.ContentHandler):
    def startElement(self, name, attrs):
        if name == "row":
            if attrs["PostTypeId"] == "2":
                insert_answer(
                    questions,
                    int(attrs["ParentId"]),
                    int(attrs["Id"]),
                    attrs["Body"],
                    dateutil.parser.parse(attrs["LastActivityDate"]),
                    int(attrs["Score"])
                )

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
