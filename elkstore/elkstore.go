package elkstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/mottaquikarim/esquerydsl"
)

type ElkDocument interface {
	DocumentID() string
}

type ElkStore struct {
	ElkClient *elasticsearch.Client
	IndexName string
}

func (s *ElkStore) CreateIndex(mapping string) error {
	res, err := s.ElkClient.Indices.Create(
		s.IndexName,
		s.ElkClient.Indices.Create.WithBody(strings.NewReader(mapping)),
	)
	if err != nil {
		return err
	}

	if res.IsError() {
		return fmt.Errorf("error: %s", res)
	}

	return nil
}

func (s *ElkStore) Create(item ElkDocument) error {
	payload, err := json.Marshal(item)
	if err != nil {
		return err
	}

	ctx := context.Background()
	res, err := esapi.CreateRequest{
		Index:      s.IndexName,
		DocumentID: item.DocumentID(),
		Body:       bytes.NewReader(payload),
		// Refresh:    "true",
	}.Do(ctx, s.ElkClient)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return err
		}
		err := e["error"].(map[string]interface{})
		return fmt.Errorf("[%s] %s: %s", res.Status(), err["type"], err["reason"])
	}

	return nil
}

func (s *ElkStore) Exists(id string) (bool, error) {
	res, err := s.ElkClient.Exists(s.IndexName, id)
	if err != nil {
		return false, err
	}

	switch res.StatusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("[%s]", res.Status())
	}
}

func (s *ElkStore) Delete(id string) (bool, error) {
	res, err := s.ElkClient.Delete(s.IndexName, id)
	if err != nil {
		return false, err
	}

	switch res.StatusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("[%s]", res.Status())
	}
}

type hits[T any] struct {
	Source *T `json:"_source"`
}
type hitsWrap[T any] struct {
	Hits []*hits[T] `json:"hits"`
}
type result[T any] struct {
	Hits *hitsWrap[T] `json:"hits"`
}

func Search[T any](s *ElkStore, queryDoc *esquerydsl.QueryDoc, arrayPtrOut *[]*T) error {
	queryDoc.Index = s.IndexName
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(queryDoc); err != nil {
		return err
	}
	res, err := s.ElkClient.Search(
		s.ElkClient.Search.WithIndex(s.IndexName),
		s.ElkClient.Search.WithBody(&buf),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return err
		}
		return fmt.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
	}

	var r result[T]
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return err
	}

	for _, hit := range r.Hits.Hits {
		*arrayPtrOut = append(*arrayPtrOut, hit.Source)
	}

	return nil
}
