package main

import (
	"context"
	"crypto/tls"
	"elastic_test/elkstore"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/mottaquikarim/esquerydsl"
)

func main() {

	cfg := elasticsearch.Config{
		Addresses: []string{"http://127.0.0.1:9200"},
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS11,
			},
		},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		panic(err)
	}

	resp, err := es.Info()
	if err != nil {
		panic(err)
	}
	fmt.Println(resp)
	// => panic: dial tcp: i/o timeout

	repo := NewElkPlanetRepo(es)
	elkstore.Create(context.Background(),
		repo.ElkStore,
		&Planet{
			PlanetID: "999",
			Name:     "Earth",
			Stage:    "beta",
			Status:   "active",
		})
	planets := []*Planet{}
	err = elkstore.Search(context.Background(),
		repo.ElkStore, &esquerydsl.QueryDoc{
			And: []esquerydsl.QueryItem{
				{Field: "planet_id", Value: "999", Type: esquerydsl.Match},
			},
		}, &planets)
	if err != nil {
		panic(err)
	}
	fmt.Println(planets)

}
