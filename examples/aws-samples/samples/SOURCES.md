# Where these samples come from

These are copies of AWS's own sample code, kept so that lambda2eks is tested against code it did not write.

| Folder | Source | Licence |
|---|---|---|
| apigw-http-api-lambda, apigw-rest-api-lambda-python, lambda-function-url, sqs-lambda-python-sam, sns-lambda-nodejs-sam, dynamodb-lambda, kinesis-lambda, eventbridge-scheduled-lambda | [aws-samples/serverless-patterns](https://github.com/aws-samples/serverless-patterns) at `23b5a9ec6b25c30268bae70b334b0dffca74d7ee` | MIT-0 ([LICENSE-serverless-patterns](LICENSE-serverless-patterns)); some source headers say Apache-2.0 |
| sam-quickstart-s3 | [aws/aws-sam-cli-app-templates](https://github.com/aws/aws-sam-cli-app-templates) `nodejs/s3` at `225a147bdd4ab23fa42583b2a708aebabbb93694`, rendered for nodejs24.x on arm64 | Apache-2.0 ([LICENSE-aws-sam-cli-app-templates](LICENSE-aws-sam-cli-app-templates)) |

Changes from the originals:
- Images and `example-pattern.json` were left out.
- sam-quickstart-s3: the cookiecutter placeholders were filled in, and its tests and buildspec were left out.

The code itself is unchanged.
