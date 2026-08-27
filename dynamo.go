package main

import (
	"context"
	"fmt"
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
		o.BaseEndpoint = aws.String("http://localhost:8000")
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

// Dequeue to find oldest available task and to lease it
func (q *DynamoQueue) Dequeue(ctx context.Context, workerID string) (*Task, error) {
	queryOut, err := q.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(q.tableName),
		IndexName:              aws.String("State-CreatedAt-Index"),
		KeyConditionExpression: aws.String("#state = :availableState"),
		ExpressionAttributeNames: map[string]string{
			"#state": "State",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":availableState": &types.AttributeValueMemberN{Value: "0"}, // Value 0 is our Available State
		},
		Limit: aws.Int32(1), // Oldest task
	})

	if err != nil {
		return nil, fmt.Errorf("failed to query GSI: %v", err)
	}
	// empty queue
	if len(queryOut.Items) == 0 {
		return nil, nil
	}

	var task Task
	if err := attributevalue.UnmarshalMap(queryOut.Items[0], &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %v", err)
	}

	visibleAt := time.Now().Add(5 * time.Second)
	updateOut, err := q.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(q.tableName),
		Key: map[string]types.AttributeValue{
			"ID": &types.AttributeValueMemberS{Value: task.ID},
		},
		// State to leased, assign the task to the ID, and set the timeout
		UpdateExpression: aws.String("SET #state = :leasedState, WorkerID = :worker, VisibleAt = :visible"),
		// Only works if the state is available
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
		// task taken
		return nil, nil
	}
	// return leased task
	var updatedTask Task
	if err := attributevalue.UnmarshalMap(updateOut.Attributes, &updatedTask); err != nil {
		return nil, fmt.Errorf("failed to unmarshal updated task: %v", err)
	}

	return &updatedTask, nil
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
