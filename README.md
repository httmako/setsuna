# setsuna

Work-In-Progress Minimalistic logging server using postgresql and golang

The whole stack is the SEK stack, consisting of setsuna, (selkie,) effie and kagero.  
They are similar to elasticsearch (log-server), filebeat (log-collector) and kibana (ui-dashboard). Selkie is the log-cleanup tool which deletes old logs after a configured time.  


# TODO

The code is a big work in progress and the kagero UI is barely working.  
Current priorities are getting the performence up to speed, then security, code quality and UI.

 - document k8s setup
 - document/implement logging users for kagero ui (using oauth2-proxy)
 - implement "-c" flag for configuring config.yaml location on all components
 - optimize setsuna's json parsing (main performance overhead)
 - optimize effie so that 9k/s is not a roadblock
 - make selkie configurable to run as a daemon or cronjob (or add to setsuna)
 - decide on json or fmt logging for all components
 - decide if 2 js transformers are necessary


# Install

Simply download all 4 components, change their config.yaml and start them up.  
Selkie is the only exception as it should run as a cronjob.


# Building

Every component (selkie, kagero, effie) can be built using a default go build command like:

    CGO_ENABLED=0 goos=linux goarch=amd64 go build .

For setsuna itself you have to enable jsonv2 (and should enable greentea gc) like this:

    GOEXPERIMENT=greenteagc,jsonv2 CGO_ENABLED=0 goos=linux goarch=amd64 go build .


# Components

## Setsuna

Setsuna is the "elasticsearch" of this stack. It receives loglines (as json arrays) from the collectors (effie) and saves them into the postgresql database.  
It uses jsonb to make the json data searchable.  
It currently offers a single endpoint (POST /v1/effie/logs) that stores the body (json arrays).

Effie will retry to send the logs to setsuna until it succeeds. This means that setsuna can be safely restarted without loosing any logs, as the "jote.RunMux" function will wait until all current connections are finished and won't accept new ones during this pre-shutdown time.

## Effie

This is the "beat" (data collector) that sends data to setsuna.  
It currently offers a file collector, which tails files matched by a pattern and sends the tail'd lines to setsuna.

It automatically saves the progress of each tailed file to a "progress.json" file. This happens after every POST to the setsuna server.  
This means you can safely restart the application after it sent out logs, as no data will be sent twice and no data will be lost.  
(The only way for data to be lost is if a logfile receives data after effie was stopped and then the file gets truncated, loosing those new lines only)

## Kagero

This is the "kibana" (ui dashboard) to view and search through the stored logs.

## Selkie

This is the cleanup routine.  
It is outside setsuna so that it can be configured, deployed and run independently.


# Performance and technical

The Javascript module effie uses is the same as the one filebeat uses. 100 log lines parsed without the javascript module takes ~500μs and with it ~2000μs, the round trip and saving of the logs on setsuna takes ~300ms so the javascript execution time is negligable.

The main bottleneck for setsuna is the json parsing.  
If you configure effie to send batches of 10.000 lines, setsuna can save them in ~240ms. Effie can read 10k lines (with javascript parsing) in 1.3s.  
If you configure effie to send in 100.000 line batches, setsuna takes ~1.8s to save them. Effie takes around 10s to read in the lines.  
With 6 concurrent effie deployments constantly sending 10k batches all at once the setsuna server keeps an average ~500ms response time.  

This means you can have around ~9000 logs per effie deployment per second (= 9k per kubernetes node if you have effie deployed as a daemonset).  
Setsuna can handle ~60.000 logs per second (6 effie deployments sending at once has an average ~500ms response time).  

If you create a simple go file that only sends 100.000 docs to postgres (same json and using CopyIn with pq library) it takes ~840ms to save it. This means (roughly) half of the time that setsuna spends per 100k batch request is being used to parse json.


