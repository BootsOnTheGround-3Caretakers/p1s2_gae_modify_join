#!/bin/bash

PROJECT=aqueous-choir-160420
gcloud -q --project=$PROJECT tasks queues create p1s2t4-add-modify-need-to-needer

gcloud -q --project=$PROJECT app deploy --version 1 3>> upload_log.txt
