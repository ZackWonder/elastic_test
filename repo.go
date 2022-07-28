package main

import (
	"elastic_test/esstore"

	"github.com/elastic/go-elasticsearch/v8"
)

type ESPlanetRepo struct {
	*esstore.ESStore
}

func NewElkPlanetRepo(client *elasticsearch.Client) *ESPlanetRepo {
	return &ESPlanetRepo{
		ESStore: &esstore.ESStore{
			IndexName: "planet",
			ESClient:  client,
		},
	}
}
