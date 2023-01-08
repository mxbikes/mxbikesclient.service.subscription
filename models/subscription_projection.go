package models

import (
	"time"

	protobuffer "github.com/mxbikes/protobuf/subscription"
)

type Subscription_projection struct {
	LastEventId int        `json:"lastEventId"`
	ModID       string     `gorm:"primaryKey;type:uuid;not null;default:null" validate:"required"`
	UserID      string     `gorm:"primaryKey;type:varchar(50);not null;default:null" validate:"required"`
	CreatedAt   time.Time  `json:"-"`
	UpdatedAt   time.Time  `json:"-"`
	DeletedAt   *time.Time `json:"-"`
}

func SubscriptionToProto(subscription *Subscription_projection) *protobuffer.Subscription {
	return &protobuffer.Subscription{
		UserID: subscription.UserID,
		ModID:  subscription.ModID,
	}
}

func SubscriptionsToProto(subscriptions []*Subscription_projection) []*protobuffer.Subscription {
	result := make([]*protobuffer.Subscription, 0, len(subscriptions))
	for _, projection := range subscriptions {
		result = append(result, SubscriptionToProto(projection))
	}
	return result
}
