#!/bin/bash

mongoexport -d stackdb -c questions -q '{$query: {question_id: {$mod: 0, 0]}}}' -o sample.json
