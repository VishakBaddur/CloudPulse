"""The deployed resources match what the processor relies on."""

import json


def test_ingest_queue_redrives_to_dlq(sqs, stack):
    attrs = sqs.get_queue_attributes(QueueUrl=stack["queue_url"], AttributeNames=["All"])["Attributes"]
    policy = json.loads(attrs["RedrivePolicy"])
    dlq_arn = sqs.get_queue_attributes(QueueUrl=stack["dlq_url"], AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    assert policy["deadLetterTargetArn"] == dlq_arn
    assert int(policy["maxReceiveCount"]) == 5


def test_visibility_timeout_covers_lambda_timeout(sqs, stack):
    attrs = sqs.get_queue_attributes(
        QueueUrl=stack["queue_url"], AttributeNames=["VisibilityTimeout"]
    )["Attributes"]
    assert int(attrs["VisibilityTimeout"]) >= 6 * stack["function_config"]["Timeout"]


def test_events_table_keyed_on_event_id_with_ttl(ddb, stack):
    table = ddb.describe_table(TableName=stack["events_table"])["Table"]
    assert table["KeySchema"] == [{"AttributeName": "event_id", "KeyType": "HASH"}]
    ttl = ddb.describe_time_to_live(TableName=stack["events_table"])["TimeToLiveDescription"]
    assert ttl["TimeToLiveStatus"] in ("ENABLED", "ENABLING")
    assert ttl["AttributeName"] == "expires_at"


def test_aggregates_table_key_schema(ddb, stack):
    table = ddb.describe_table(TableName=stack["aggregates_table"])["Table"]
    keys = {k["AttributeName"]: k["KeyType"] for k in table["KeySchema"]}
    assert keys == {"source": "HASH", "bucket_key": "RANGE"}


def test_function_configuration(stack):
    cfg = stack["function_config"]
    assert cfg["Runtime"] == "python3.13"
    assert cfg["Handler"] == "processor.handler.handler"
    env = stack["function_env"]
    for key in ("EVENTS_TABLE", "AGGREGATES_TABLE", "DLQ_URL", "METRICS_NAMESPACE"):
        assert env.get(key), f"{key} not set on the function"


def test_event_source_mapping_reports_partial_failures(lambda_client, stack):
    mappings = lambda_client.list_event_source_mappings(
        FunctionName=stack["function_name"], EventSourceArn=stack["queue_arn"]
    )["EventSourceMappings"]
    assert len(mappings) == 1
    esm = mappings[0]
    assert esm["State"] in ("Enabled", "Enabling", "Updating")
    assert esm["BatchSize"] == 10
    assert "ReportBatchItemFailures" in esm.get("FunctionResponseTypes", [])
