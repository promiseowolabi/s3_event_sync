from platform import node
from constructs import Construct
from aws_cdk import (
    Arn,
    Duration,
    Stack,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_datasync as datasync,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda_event_sources as sqssource,
    aws_s3 as s3,
)


class S3EventSyncStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 manifest bucket
        manifest_bucket = s3.Bucket(
            self, 's3cdkdatasyncmanifest', 
            bucket_name='s3cdkdatasyncmanifest',
        )

        # S3 source bucket - Enable EventBridge
        source_cfn_bucket = s3.CfnBucket(
            self, 's3cdkdatasyncsource', 
            bucket_name='s3cdkdatasyncsource',
            )

        source_cfn_bucket.add_property_override(
            'NotificationConfiguration.EventBridgeConfiguration.EventBridgeEnabled', True
        )

        # S3 destination bucket
        destination_bucket = s3.Bucket(
            self, 's3cdkdatasyncdestination', 
            bucket_name='s3cdkdatasyncdestination',
        )

        # Source location role
        source_location_role = iam.Role(self, 'SourceLocationRole',
            assumed_by=iam.ServicePrincipal('datasync.amazonaws.com'),
        )

        source_location_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads"],
                resources=[source_cfn_bucket.attr_arn]
            )
        )
        source_location_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                "s3:AbortMultipartUpload",
                "s3:DeleteObject",
                "s3:GetObject",
                "s3:ListMultipartUploadParts",
                "s3:PutObjectTagging",
                "s3:GetObjectTagging",
                "s3:PutObject"],
                resources=[source_cfn_bucket.attr_arn+'/*']
            )
        )

         # Destination location role
        destination_location_role = iam.Role(self, id='DestinationLocationRole',
            assumed_by=iam.ServicePrincipal('datasync.amazonaws.com'),
        )

        destination_location_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads"],
                resources=[destination_bucket.bucket_arn]
            )
        )
        destination_location_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                "s3:AbortMultipartUpload",
                "s3:DeleteObject",
                "s3:GetObject",
                "s3:ListMultipartUploadParts",
                "s3:PutObjectTagging",
                "s3:GetObjectTagging",
                "s3:PutObject"],
                resources=[destination_bucket.bucket_arn+'/*']
            )
        )

        # S3 source location 
        source_location = datasync.CfnLocationS3(
            self, 'SourceLocation',
            s3_bucket_arn=source_cfn_bucket.attr_arn,
            s3_config=datasync.CfnLocationS3.S3ConfigProperty(
                bucket_access_role_arn=source_location_role.role_arn
            )
        )

        # S3 destination location
        destination_location = datasync.CfnLocationS3(
            self, 'DestinationLocation',
            s3_bucket_arn=destination_bucket.bucket_arn,
            s3_config=datasync.CfnLocationS3.S3ConfigProperty(
                bucket_access_role_arn=destination_location_role.role_arn
            )
        )

        # DataSync Task from source to destination
        ds_task = datasync.CfnTask(self, "DataSyncTask",
            destination_location_arn=destination_location.attr_location_arn,
            source_location_arn=source_location.attr_location_arn,
        )

        # SQS Queue creation
        queue = sqs.Queue(
            self, "S3EventSyncQueue",
            visibility_timeout=Duration.seconds(300),
        )

        # Sync Function
        sync_function = _lambda.Function(
            self, 'SyncHandler',
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset('lambda'),
            handler='s3sync.handler',
            reserved_concurrent_executions=1,
            environment={
                'manifest_bucket_name': manifest_bucket.bucket_name,
                'datasync_task_arn': ds_task.attr_task_arn,
            },
        )

        # Lambda SQS trigger, batch size and window
        sync_function.add_event_source(sqssource.SqsEventSource(queue,
            batch_size=100,
            max_batching_window=Duration.seconds(30),
        ))

        sync_function.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                'AmazonSQSFullAccess'
            )
        )

        sync_function.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                'AmazonS3FullAccess'
            )
        )

        sync_function.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                'AWSDataSyncFullAccess'
            )
        )

        # Lambda EventBridge Scheduled trigger
        schedule_rule = events.Rule(
            self, 'ScheduleRule',
            schedule=events.Schedule.rate(Duration.minutes(10)),
        )
        schedule_rule.add_target(targets.LambdaFunction(sync_function,
            event=events.RuleTargetInput.from_object({"triggeredBy": "eventbridge"})
        ))

        # EventBridge S3 Created Object rule
        s3_rule = events.Rule(
            self, 'S3EventsRule',
            targets=[targets.SqsQueue(queue)],
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={"bucket": {"name": [source_cfn_bucket.bucket_name]}}
            ),
        )

    def get_manifest_bucket(self):
        return self.manifest_bucket.bucket_name

    def get_task_arn(self):
        return self.ds_task.attr_task_arn

