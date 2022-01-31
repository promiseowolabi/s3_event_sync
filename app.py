#!/usr/bin/env python3

import aws_cdk as cdk

from s3_event_sync.s3_event_sync_stack import S3EventSyncStack


app = cdk.App()
S3EventSyncStack(app, "s3-event-sync")

app.synth()
