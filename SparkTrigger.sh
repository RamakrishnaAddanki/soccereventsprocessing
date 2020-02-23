#!/usr/bin/env bash


spark-submit  \
    --name footballevents \
    --master local[4] \
    --driver-memory 1g \
    --py-files /Users/ramakrishnaaddanki/PycharmProjects/soccereventsprocessing/dist/pyspark_ci_demo-0.1-py3.7.egg \
    /Users/ramakrishnaaddanki/PycharmProjects/soccereventsprocessing/__main__.py