package repository

import (
	"github.com/mxbikes/mxbikesclient.service.subscription/models"
	"gorm.io/gorm"
)

type SubscriptionRepository interface {
	SearchByUserID(userID string) ([]*models.Subscription_projection, error)
	Save(comment *models.Subscription_projection) error
	Delete(userID string, modID string) error
	Migrate() error
}

type postgresRepository struct {
	db *gorm.DB
}

func NewRepository(c *gorm.DB) *postgresRepository {
	return &postgresRepository{db: c}
}

func (p *postgresRepository) SearchByUserID(userID string) ([]*models.Subscription_projection, error) {
	var l []*models.Subscription_projection
	err := p.db.Where(`user_id = ?`, userID).Find(&l).Error
	return l, err
}

func (p *postgresRepository) Save(comment *models.Subscription_projection) error {
	return p.db.Save(comment).Error
}

func (p *postgresRepository) Delete(userID string, modID string) error {
	return p.db.Delete(&models.Subscription_projection{UserID: userID, ModID: modID}).Error
}

func (p *postgresRepository) Migrate() error {
	p.db.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`)
	return p.db.AutoMigrate(&models.Subscription_projection{})
}
