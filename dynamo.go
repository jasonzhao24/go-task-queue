package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamoQueue struct {
	client    *dynamodb.Client
	tableName string
}

// Point to docker
func NewDynamoQueue(ctx context.Context, tableName string) (*DynamoQueue, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	if err != nil {
		return nil, err
	}
	// DynamoDB client
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		endpoint := os.Getenv("DYNAMODB_ENDPOINT")
		if endpoint == "" {
			endpoint = "http://dynamodb-local:8000" // Use the Docker Compose service name
		}
		o.BaseEndpoint = aws.String(endpoint)
	})

	return &DynamoQueue{
		client:    client,
		tableName: tableName,
	}, nil
}

// Enqueue function in main added into DynamoDB table
func (q *DynamoQueue) Enqueue(ctx context.Context, payload []byte) (string, error) {
	// ID and Time
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	now := time.Now().Unix()
	// Create Task
	newTask := Task{
		ID:         id,
		Payload:    payload,
		State:      Available,
		RetryCount: 0,
		CreatedAt:  now,
	}

	item, err := attributevalue.MarshalMap(newTask)
	if err != nil {
		return "", err
	}
	// database request
	_, err = q.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(q.tableName),
		Item:      item,
	})
	// Catch error
	if err != nil {
		return "", fmt.Errorf("failed to put item in DynamoDB: %v", err)
	}

	return id, nil
}

func (q *DynamoQueue) Dequeue(ctx context.Context, workerID string) (*Task, error) {
	queryOut, err := q.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(q.tableName),
		IndexName:              aws.String("State-CreatedAt-Index"),
		KeyConditionExpression: aws.String("#state = :availableState"),
		ExpressionAttributeNames: map[string]string{
			"#state": "State",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":availableState": &types.AttributeValueMemberN{Value: "0"},
		},
		Limit: aws.Int32(5),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to query GSI: %v", err)
	}

	if len(queryOut.Items) == 0 {
		return nil, nil // Queue is completely empty
	}

	// iterate through batgch for tasks
	for _, item := range queryOut.Items {
		var task Task
		if err := attributevalue.UnmarshalMap(item, &task); err != nil {
			continue
		}

		visibleAt := time.Now().Add(5 * time.Second)
		updateOut, err := q.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(q.tableName),
			Key: map[string]types.AttributeValue{
				"ID": &types.AttributeValueMemberS{Value: task.ID},
			},
			UpdateExpression:    aws.String("SET #state = :leasedState, WorkerID = :worker, VisibleAt = :visible"),
			ConditionExpression: aws.String("#state = :availableState"),
			ExpressionAttributeNames: map[string]string{
				"#state": "State",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":leasedState":    &types.AttributeValueMemberN{Value: "1"},
				":worker":         &types.AttributeValueMemberS{Value: workerID},
				":visible":        &types.AttributeValueMemberS{Value: visibleAt.Format(time.RFC3339Nano)},
				":availableState": &types.AttributeValueMemberN{Value: "0"},
			},
			ReturnValues: types.ReturnValueAllNew,
		})

		if err != nil {
			// find another task
			continue
		}

		var updatedTask Task
		if err := attributevalue.UnmarshalMap(updateOut.Attributes, &updatedTask); err != nil {
			return nil, fmt.Errorf("failed to unmarshal updated task: %v", err)
		}

		return &updatedTask, nil
	}

	// No available tasks
	return nil, nil
}

// Create DynamoDB table
func (q *DynamoQueue) CreateTable(ctx context.Context) error {
	_, err := q.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(q.tableName),
		// define attributes
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("ID"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("State"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("CreatedAt"), AttributeType: types.ScalarAttributeTypeN},
		},
		// Access using ID
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("ID"), KeyType: types.KeyTypeHash},
		},
		// Oldest task
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("State-CreatedAt-Index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("State"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("CreatedAt"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				},
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})

	return err
}

func (q *DynamoQueue) Acknowledge(ctx context.Context, taskID string) error {
	_, err := q.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(q.tableName),
		// Get ID to delete row in DynamoDB table
		Key: map[string]types.AttributeValue{
			"ID": &types.AttributeValueMemberS{Value: taskID},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to delete task %s: %v", taskID, err)
	}

	return nil
}

// Scans for expired leases and resets them
func (q *DynamoQueue) StartReaper(ctx context.Context) {
	for {
		time.Sleep(2 * time.Second)

		out, err := q.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(q.tableName),
			IndexName:              aws.String("State-CreatedAt-Index"),
			KeyConditionExpression: aws.String("#state = :leasedState"),
			ExpressionAttributeNames: map[string]string{
				"#state": "State",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":leasedState": &types.AttributeValueMemberN{Value: "1"}, // 1 = Leased
			},
		})

		if err != nil {
			fmt.Printf("[Reaper] Error querying GSI: %v\n", err)
			continue
		}

		now := time.Now()

		// 2. Check if expired
		for _, item := range out.Items {
			var task Task
			if err := attributevalue.UnmarshalMap(item, &task); err != nil {
				continue
			}

			if now.After(task.VisibleAt) {
				_, err := q.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String(q.tableName),
					Key: map[string]types.AttributeValue{
						"ID": &types.AttributeValueMemberS{Value: task.ID},
					},
					UpdateExpression: aws.String("SET #state = :availableState, WorkerID = :emptyWorker, RetryCount = RetryCount + :inc"),

					ConditionExpression: aws.String("#state = :leasedState"),
					ExpressionAttributeNames: map[string]string{
						"#state": "State",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":availableState": &types.AttributeValueMemberN{Value: "0"},
						":emptyWorker":    &types.AttributeValueMemberS{Value: ""},
						":inc":            &types.AttributeValueMemberN{Value: "1"},
						":leasedState":    &types.AttributeValueMemberN{Value: "1"},
					},
				})

				if err == nil {
					fmt.Printf("[Reaper] Lease expired for task %s. Resetting to Available. (Retries: %d)\n", task.ID, task.RetryCount+1)
				}
			}
		}
	}
}
