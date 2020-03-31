# DynamoDB

A leaky bucket backed by AWS DynamoDB

## Usage

This library assumes the table is created with the following attributes:

- `name` is the primary key
- (optional) `_ttl` is the enabled time to live specification field

Depending on your use case it may be worth disabling the default retries in the passed in `aws.Config` object. Please refer to the following sections for examples.

### CloudFormation Table Definition Example

```yaml
  Buckets:
    Type: AWS::DynamoDB::Table
    Properties:
      AttributeDefinitions:
      - AttributeName: name
        AttributeType: S
      BillingMode: PAY_PER_REQUEST
      KeySchema:
      - AttributeName: name
        KeyType: HASH
      SSESpecification:
        SSEEnabled: true
      TableName: your-favorite-table-naming-strategy-Buckets
      TimeToLiveSpecification:
        AttributeName: _ttl
        Enabled: true
```

### Usage Example

``` golang
package main

import (
    "log"
    "time"

    leakybucketDynamoDB "github.com/Clever/leakybucket/dynamodb"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
)

func main() {
    storage, err := leakybucketDynamoDB.New(
        "buckets-table",
        session.New(&aws.Config{
            Region:     aws.String("us-west-1"),
            MaxRetries: aws.Int(0),
        }),
        24*time.Hour,
    )
    if err != nil {
        log.Fatal(err)
    }

    bucket, err := storage.Create("my-bucket", 100, time.Minute)
    if err != nil {
        log.Fatal(err)
    }

    // use your new leaky bucket!
}
```

### Testing

All tests assume there is a locally running DynamoDB defined in an environment variable `AWS_DYNAMO_ENDPOINT`

## Helpful Development Resources

- [DynamoDB CloudFormation User Guide](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-dynamodb-table.html)
- [DynamoDB Conditional Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html)
- [DynamoDB Update Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html)
