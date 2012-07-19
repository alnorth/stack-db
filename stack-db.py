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
        "accepted_answer_id": accepted_answer_id
    }
    questions.insert(question)
    print id

class SOProcessor(handler.ContentHandler):

    def startElement(self, name, attrs):
        if name == "row":
            if attrs["PostTypeId"] == "1":
                global questions
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

parser = make_parser()
parser.setContentHandler(SOProcessor())
parser.parse(open(sys.argv[1]))
