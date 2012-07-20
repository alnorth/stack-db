from bson.code import Code
import dateutil.parser
from pymongo import Connection
import sys
import time
from xml.sax import make_parser, handler

# Set up the database connection
connection = Connection()
db = connection.stackdb

if len(sys.argv) == 2:
    # Set up a temporary collection for insertion
    tmp_posts = db.tmp_posts
    # Make sure it's empty
    tmp_posts.remove()

    tmp_posts.ensure_index("question_id", unique=True)

    class SOProcessor(handler.ContentHandler):
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
                        "answers": [answer]
                    }
                    tmp_posts.insert(question)

                if int(attrs["Id"]) % 5000 == 0:
                    print attrs["Id"]

    # We insert everything to start with, then use Map/Reduce to bring answers and questions together

    print "Importing posts ..."
    qparser = make_parser()
    qparser.setContentHandler(SOProcessor())
    qparser.parse(open(sys.argv[1]))

    print "Running Map/Reduce ..."
    map = Code("function () { emit(this.question_id, this); }")
    reduce = Code("function (question_id, questions) {"
                    "   var result = {};"
                    "   questions.forEach(function(q) {"
                    "       result.title = result.title || q.title;"
                    "       result.body = result.body || q.body;"
                    "       result.tags = result.tags || q.tags;"
                    "       result.last_activity_date = result.last_activity_date || q.last_activity_date;"
                    "       result.score = result.score || q.score;"
                    "       result.accepted_answer_id = result.accepted_answer_id || q.accepted_answer_id;"
                    "       result.answers = result.answers.concat(q.answers);"
                    "   });"
                    "   return result;"
                    "}")
    questions = db.tmp_posts.map_reduce(map, reduce, "questions")
    print "Questions - %s" % questions.count()

    db.tmp_posts.remove()
