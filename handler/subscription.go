package handler

import (
	"context"
	"encoding/json"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/go-playground/validator"
	"github.com/mxbikes/mxbikesclient.service.subscription/models"
	"github.com/mxbikes/mxbikesclient.service.subscription/repository"
	protobuffer "github.com/mxbikes/protobuf/subscription"
	"github.com/sirupsen/logrus"
)

type Subscription struct {
	protobuffer.UnimplementedSubscriptionServiceServer
	repository repository.SubscriptionRepository
	logger     logrus.Logger
	validate   *validator.Validate
	db         *esdb.Client
}

const log_withID = "subscription with userID: {%s} "

// Return a new handler
func New(postgres repository.SubscriptionRepository, logger logrus.Logger, db *esdb.Client) *Subscription {
	return &Subscription{repository: postgres, validate: validator.New(), logger: logger, db: db}
}

func (e *Subscription) GetSubscriptionsByUserID(ctx context.Context, req *protobuffer.GetSubscriptionsByIDRequest) (*protobuffer.GetSubscriptionsByIDResponse, error) {
	// Get Requested Subscriptions
	subscriptions, err := e.repository.SearchByUserID(req.UserID)
	if err != nil {
		return nil, err
	}

	e.logger.WithFields(logrus.Fields{"prefix": "SERVICE.Subscription_GetSubscriptionsByUserID"}).Infof(log_withID, req.UserID)

	return &protobuffer.GetSubscriptionsByIDResponse{Subscriptions: models.SubscriptionsToProto(subscriptions)}, nil
}

func (e *Subscription) AddSubscription(ctx context.Context, req *protobuffer.AddSubscriptionRequest) (*protobuffer.AddSubscriptionResponse, error) {
	// Request to JSON
	subscription, err := json.Marshal(&models.Subscription{ModID: req.ModID})
	if err != nil {
		return nil, err
	}

	// Create Event
	eventData := esdb.EventData{
		ContentType: esdb.JsonContentType,
		EventType:   "SUBSCRIPTION_ADDED",
		Data:        subscription,
	}

	// Append Event To stream
	_, err = e.db.AppendToStream(context.Background(), "subscription-"+req.UserID, esdb.AppendToStreamOptions{}, eventData)
	if err != nil {
		return nil, err
	}

	e.logger.WithFields(logrus.Fields{"prefix": "SERVICE.Subscription_AddSubscription"}).Infof(log_withID, req.UserID)

	return &protobuffer.AddSubscriptionResponse{}, nil
}

func (e *Subscription) RemoveSubscription(ctx context.Context, req *protobuffer.RemoveSubscriptionRequest) (*protobuffer.RemoveSubscriptionResponse, error) {
	// Request to JSON
	subscription, err := json.Marshal(&models.Subscription{ModID: req.ModID})
	if err != nil {
		return nil, err
	}

	// Create Event
	eventData := esdb.EventData{
		ContentType: esdb.JsonContentType,
		EventType:   "SUBSCRIPTION_REMOVED",
		Data:        subscription,
	}

	// Append Event To stream
	_, err = e.db.AppendToStream(context.Background(), "subscription-"+req.UserID, esdb.AppendToStreamOptions{}, eventData)
	if err != nil {
		return nil, err
	}

	e.logger.WithFields(logrus.Fields{"prefix": "SERVICE.Subscriptions_RemoveSubscription"}).Infof(log_withID, req.UserID)

	return &protobuffer.RemoveSubscriptionResponse{}, nil
}
