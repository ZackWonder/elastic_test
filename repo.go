package main

import (
	"elastic_test/elkstore"

	"github.com/elastic/go-elasticsearch/v8"
)

type ElkPlanetRepo struct {
	*elkstore.ElkStore
}

func NewElkPlanetRepo(client *elasticsearch.Client) *ElkPlanetRepo {
	return &ElkPlanetRepo{
		ElkStore: &elkstore.ElkStore{
			IndexName: "planet",
			ElkClient: client,
		},
	}
}
