#!/bin/bash

PROJECT=aqueous-choir-160420

gcloud -q --project=$PROJECT tasks queues create p1s2t1-add-modify-cluster-user
gcloud -q --project=$PROJECT tasks queues create p1s2t2-remove-user-from-cluster
gcloud -q --project=$PROJECT tasks queues create p1s2t3-add-modify-user-skill
gcloud -q --project=$PROJECT tasks queues create p1s2t4-add-modify-need-to-needer
gcloud -q --project=$PROJECT tasks queues create p1s2t5-remove-need-from-needer
gcloud -q --project=$PROJECT tasks queues create p1s2t6-remove-needer-from-user
gcloud -q --project=$PROJECT tasks queues create p1s2t7-assign-hashtag-to-user
gcloud -q --project=$PROJECT tasks queues create p1s2t8-remove-hashtag-from-user
gcloud -q --project=$PROJECT tasks queues create p1s2t9-remove-skill-from-user
gcloud -q --project=$PROJECT tasks queues create p1s2t10-modify-user-information

gcloud -q --project=$PROJECT app deploy --version 1 3>> upload_log.txt
