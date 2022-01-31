import aws_cdk as core
import aws_cdk.assertions as assertions
from s3_event_sync.s3_event_sync_stack import S3EventSyncStack


def test_sqs_queue_created():
    app = core.App()
    stack = S3EventSyncStack(app, "s3-event-sync")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::SQS::Queue", {
        "VisibilityTimeout": 300
    })


def test_sns_topic_created():
    app = core.App()
    stack = S3EventSyncStack(app, "s3-event-sync")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::SNS::Topic", 1)
