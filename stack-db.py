from bson.code import Code
import dateutil.parser
import math
import os
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
    tmp_posts.drop()

    tmp_posts.ensure_index("question_id")

    class SOProcessor(handler.ContentHandler):

        def __init__(self, pf):
            handler.ContentHandler.__init__(self)
            self.pf = pf

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
            hashes = int(math.floor(self.percentage)/2)
            text = "\r[{0}{1}] {2}%".format("#" * hashes, " " * (50 - hashes), round(self.percentage, 1))
            sys.stdout.write(text)
            sys.stdout.flush()

    # We insert everything to start with, then use Map/Reduce to bring answers and questions together

    print "Importing posts ..."
    qparser = make_parser()
    pf = PercentageFile(sys.argv[1])
    qparser.setContentHandler(SOProcessor(pf))
    qparser.parse(pf)

    print ""
    print "Running Map/Reduce ..."
    map = Code("function () { emit(this.question_id, this); }")
    reduce = Code("function (question_id, questions) {"
                    "   var result = {answers: []};"
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
    questions = tmp_posts.map_reduce(map, reduce, "questions")
    print "Questions - %s" % questions.count()

    tmp_posts.drop()
