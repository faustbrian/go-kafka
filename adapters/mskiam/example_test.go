package mskiam_test

import (
	"context"
	"time"

	kafka "github.com/faustbrian/go-kafka"
	mskiam "github.com/faustbrian/go-kafka/adapters/mskiam"
)

func Example() {
	provider, err := mskiam.New(context.Background(), mskiam.Config{
		Region:              "eu-north-1",
		CredentialsProvider: staticCredentialsProvider{},
		TokenTimeout:        5 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	security := kafka.ClientSecurity{
		Authentication: kafka.NewOAuthBearerAuthentication(provider),
	}
	_ = security
}
