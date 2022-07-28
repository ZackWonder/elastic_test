package esstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/mottaquikarim/esquerydsl"
)

type ESDocument interface {
	DocumentID() string
}

type ESStore struct {
	ESClient  *elasticsearch.Client
	IndexName string
}

func CreateIndex(ctx context.Context, store *ESStore, mapping string) error {
	res, err := store.ESClient.Indices.Create(
		store.IndexName,
		store.ESClient.Indices.Create.WithBody(strings.NewReader(mapping)),
	)
	if err != nil {
		return err
	}

	if res.IsError() {
		return fmt.Errorf("error: %s", res)
	}
	return nil
}

func ESCreate(ctx context.Context, store *ESStore, item ESDocument) error {
	payload, err := json.Marshal(item)
	if err != nil {
		return err
	}

	res, err := store.ESClient.Create(
		store.IndexName,
		item.DocumentID(),
		bytes.NewReader(payload),
		store.ESClient.Create.WithContext(ctx),
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

func ESExists(ctx context.Context, store *ESStore, id string) (bool, error) {
	res, err := store.ESClient.Exists(store.IndexName, id, store.ESClient.Exists.WithContext(ctx))
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

func ESDelete(ctx context.Context, store *ESStore, id string) (bool, error) {
	res, err := store.ESClient.Delete(store.IndexName, id, store.ESClient.Delete.WithContext(ctx))
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

func ESSearch[T any](ctx context.Context, s *ESStore, queryDoc *esquerydsl.QueryDoc, arrayPtrOut *[]*T) error {

	queryDoc.Index = s.IndexName
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(queryDoc); err != nil {
		return err
	}
	res, err := s.ESClient.Search(
		s.ESClient.Search.WithIndex(s.IndexName),
		s.ESClient.Search.WithBody(&buf),
		s.ESClient.Search.WithContext(ctx),
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
