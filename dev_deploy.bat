set PROJECT=aqueous-choir-160420
gcloud -q --project=%PROJECT% app deploy --version 1 3>> upload_log.txt
timeout=3