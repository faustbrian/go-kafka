package mskiam_test

import (
	"context"
	"time"

	mskiam "github.com/faustbrian/go-kafka/adapters/mskiam/v2"
	kafka "github.com/faustbrian/go-kafka/v2"
)

func Example() {
	provider, err := mskiam.Load(context.Background(), mskiam.Config{
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
