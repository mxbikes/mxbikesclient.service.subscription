package main

import (
	"context"
	"log"
	"net"
	"os"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/joho/godotenv"
	"github.com/mxbikes/mxbikesclient.service.subscription/handler"
	"github.com/mxbikes/mxbikesclient.service.subscription/projection"
	"github.com/mxbikes/mxbikesclient.service.subscription/repository"
	protobuffer "github.com/mxbikes/protobuf/subscription"
	"github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	logLevel    = getEnv("LOG_LEVEL")
	port        = getEnv("PORT")
	postgresUrl = getEnv("POSTGRES_URI")
	esdbUrl     = getEnv("ESDB")
)

func main() {
	logger := &logrus.Logger{
		Out:   os.Stderr,
		Level: logrus.DebugLevel,
		Formatter: &prefixed.TextFormatter{
			TimestampFormat: "2006-01-02 15:04:05",
			FullTimestamp:   true,
			ForceFormatting: true,
		},
	}

	/* Database */
	// postgres
	db, err := gorm.Open(postgres.Open(postgresUrl), &gorm.Config{})
	if err != nil {
		logger.WithFields(logrus.Fields{"prefix": "POSTGRES"}).Fatal("unable to open a connection to database")
	}
	logger.WithFields(logrus.Fields{"prefix": "POSTGRES"}).Info("connection has been established successfully!")
	repo := repository.NewRepository(db)
	repo.Migrate()

	//db.Delete(&models.Subscription_projection{UserID: ""})

	//return

	// eventstoredatabase
	settings, err := esdb.ParseConnectionString(esdbUrl)
	if err != nil {
		panic(err)
	}
	eventStoreDB, err := esdb.NewClient(settings)

	/* Server */
	projectionHandler := projection.New(repo, *logger, eventStoreDB)
	go func() {
		err := projectionHandler.Subscribe(context.Background())
		if err != nil {
			logger.WithFields(logrus.Fields{"prefix": "POSTGRES_SUBSCRIPTION"}).Fatalf("err: {%v}", err)
		}
	}()

	// Create a tcp listener
	listener, err := net.Listen("tcp", port)
	if err != nil {
		logger.WithFields(logrus.Fields{"prefix": "SERVICE.SUBSCRIPTION"}).Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	protobuffer.RegisterSubscriptionServiceServer(grpcServer, handler.New(repo, *logger, eventStoreDB))
	reflection.Register(grpcServer)

	// Start grpc server on listener
	logger.WithFields(logrus.Fields{"prefix": "SERVICE.SUBSCRIPTION"}).Infof("is listening on Grpc PORT: {%v}", listener.Addr())
	if err := grpcServer.Serve(listener); err != nil {
		logger.WithFields(logrus.Fields{"prefix": "SERVICE.SUBSCRIPTION"}).Fatalf("failed to serve: %v", err)
	}
}

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnv(key string) string {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	return os.Getenv(key)
}
