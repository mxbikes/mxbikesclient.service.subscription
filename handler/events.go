package handler

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/mxbikes/mxbikesclient.service.subscription/models"
	"github.com/mxbikes/mxbikesclient.service.subscription/repository"
	"github.com/sirupsen/logrus"
)

type Events struct {
	repository repository.SubscriptionRepository
	logger     logrus.Logger
}

const log_withEventAggregateID = "subscription with id: {%s} "

// Return a new handler
func NewEventHandler(postgres repository.SubscriptionRepository, logger logrus.Logger) *Events {
	return &Events{repository: postgres, logger: logger}
}

func (o *Events) OnSubscriptionAdded(ctx context.Context, evt *esdb.RecordedEvent) error {
	var aggregateId = strings.ReplaceAll(evt.StreamID, "subscription-", "")

	var subscription models.Subscription
	if err := json.Unmarshal(evt.Data, &subscription); err != nil {
		return err
	}

	o.logger.WithFields(logrus.Fields{"prefix": "SERVICE.CommentEvent_OnSubscriptionAdded"}).Infof(log_withEventAggregateID, aggregateId)

	return o.repository.Save(&models.Subscription_projection{
		LastEventId: int(evt.EventNumber),
		ModID:       subscription.ModID,
		UserID:      aggregateId,
	})
}

func (o *Events) OnSubscriptionRemoved(ctx context.Context, evt *esdb.RecordedEvent) error {
	var aggregateId = strings.ReplaceAll(evt.StreamID, "subscription-", "")

	var subscription models.Subscription
	if err := json.Unmarshal(evt.Data, &subscription); err != nil {
		return err
	}

	o.logger.WithFields(logrus.Fields{"prefix": "SERVICE.CommentEvent_OnSubscriptionRemoved"}).Infof(log_withEventAggregateID, aggregateId)

	return o.repository.Delete(aggregateId, subscription.ModID)
}
