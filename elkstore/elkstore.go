package elkstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/mottaquikarim/esquerydsl"
)

type ElkDocument interface {
	DocumentID() string
}

type ElkStore struct {
	ElkClient *elasticsearch.Client
	IndexName string
}

func CreateIndex(store *ElkStore) error {
	res, err := store.ElkClient.Indices.Create(
		store.IndexName,
		// store.ElkClient.Indices.Create.WithBody(strings.NewReader(mapping)),
	)
	if err != nil {
		return err
	}

	if res.IsError() {
		return fmt.Errorf("error: %s", res)
	}
	return nil
}

func Create(ctx context.Context, store *ElkStore, item ElkDocument) error {
	payload, err := json.Marshal(item)
	if err != nil {
		return err
	}

	res, err := store.ElkClient.Create(
		store.IndexName,
		item.DocumentID(),
		bytes.NewReader(payload),
		store.ElkClient.Create.WithContext(ctx),
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
		err := e["error"].(map[string]interface{})
		return fmt.Errorf("[%s] %s: %s", res.Status(), err["type"], err["reason"])
	}

	return nil
}

func Exists(ctx context.Context, store *ElkStore, id string) (bool, error) {
	res, err := store.ElkClient.Exists(store.IndexName, id, store.ElkClient.Exists.WithContext(ctx))
	if err != nil {
		return false, err
	}

	switch res.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("[%s]", res.Status())
	}
}

func Delete(ctx context.Context, store *ElkStore, id string) (bool, error) {
	res, err := store.ElkClient.Delete(store.IndexName, id, store.ElkClient.Delete.WithContext(ctx))
	if err != nil {
		return false, err
	}

	switch res.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
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

func Search[T any](ctx context.Context, s *ElkStore, queryDoc *esquerydsl.QueryDoc, arrayPtrOut *[]*T) error {

	queryDoc.Index = s.IndexName
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(queryDoc); err != nil {
		return err
	}
	res, err := s.ElkClient.Search(
		s.ElkClient.Search.WithIndex(s.IndexName),
		s.ElkClient.Search.WithBody(&buf),
		s.ElkClient.Search.WithContext(ctx),
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
