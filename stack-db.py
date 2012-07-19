import dateutil.parser
from pymongo import Connection
import sys
from xml.sax import make_parser, handler

# Set up the database connection
connection = Connection()
db = connection.stackdb
questions = db.questions

# For now we clear the collection before we start
questions.remove()

def import_question(questions, id, title, body, tags, last_activity_date, score, answer_count, accepted_answer_id):
    question = {
        "question_id": int(id),
        "title": title,
        "body": body,
        "tags": tags,
        "last_activity_date": last_activity_date,
        "score": score,
        "answer_count": answer_count,
        "accepted_answer_id": accepted_answer_id,
        "answers": []
    }
    questions.insert(question)

def import_answer(questions, question_id, answer_id, body, last_activity_date, score):
    answer = {
        "answer_id": answer_id,
        "body": body,
        "last_activity_date": last_activity_date,
        "score": score
    }
    question = questions.find_one({"question_id": question_id})
    if question:
        question["answers"].append(answer)
        questions.update({"question_id": question_id}, question)
    else:
        print "Missing question %s" % question_id

class SOProcessor(handler.ContentHandler):

    def startElement(self, name, attrs):
        if name == "row":
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
            if int(attrs["Id"]) % 1000 == 0:
                print attrs["Id"]

parser = make_parser()
parser.setContentHandler(SOProcessor())
parser.parse(open(sys.argv[1]))
