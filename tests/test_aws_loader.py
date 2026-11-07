import json

from lambda2eks.loaders.aws import _policy_triggers

FN = "arn:aws:lambda:eu-west-1:111122223333:function:logger"


class FakeLambda:
    class exceptions:
        ResourceNotFoundException = KeyError

    def get_policy(self, FunctionName):
        # What SAM writes for an S3 event: SourceAccount, no SourceArn.
        return {"Policy": json.dumps({"Statement": [{"Principal": {"Service": "s3.amazonaws.com"},
                                                     "Condition": {"StringEquals": {"AWS:SourceAccount": "111122223333"}}}]})}


class FakeS3:
    def get_paginator(self, name):
        class P:
            def paginate(self, **kw):
                assert kw == {"BucketRegion": "eu-west-1"}
                return [{"Buckets": [{"Name": "other"}, {"Name": "uploads"}, {"Name": "locked"}]}]
        return P()

    def get_bucket_notification_configuration(self, Bucket):
        if Bucket == "locked":
            raise PermissionError
        arn = FN if Bucket == "uploads" else FN.replace("logger", "someone-else")
        return {"LambdaFunctionConfigurations": [{"LambdaFunctionArn": arn, "Events": ["s3:ObjectCreated:*"],
                                                  "Filter": {"Key": {"FilterRules": [{"Name": "suffix", "Value": ".json"}]}}}]}


def test_s3_trigger_found_without_source_arn():
    warnings = []
    triggers = _policy_triggers({"lambda": FakeLambda(), "s3": FakeS3()}, "logger", FN, warnings)
    assert [(t.kind, t.details["bucket"], t.details["filter_rules"]) for t in triggers] == \
        [("s3", "uploads", [{"Name": "suffix", "Value": ".json"}])]
    assert warnings == []


class FakeApigw2:
    class exceptions:
        NotFoundException = KeyError

    def get_api(self, ApiId):
        assert ApiId == "l28jrqc2p7"
        return {}

    def get_paginator(self, name):
        class P:
            def paginate(self, ApiId):
                return [{"Items": [{"IntegrationUri": f"arn:aws:apigateway:eu-west-1:lambda:path/2015-03-31/functions/{FN}/invocations",
                                    "PayloadFormatVersion": "2.0"}]}]
        return P()


def test_http_api_granted_on_the_whole_api_keeps_payload_2():
    from lambda2eks.loaders.aws import _apigw_trigger
    warnings = []
    [t] = _apigw_trigger(FakeApigw2(), "arn:aws:execute-api:eu-west-1:111122223333:l28jrqc2p7/*", FN, warnings)
    assert (t.details["event_format"], t.details["method"], t.details["path"], t.details["source"]) == \
        ("2.0", "ANY", None, "api-gateway-http")
    assert warnings == []
