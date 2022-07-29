package esstore

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/mottaquikarim/esquerydsl"
)

const localTest = true

var indexID = new(int32)

type TestDoc struct {
	DocID    string    `json:"doc_id" bson:"doc_id,omitempty"`
	Name     string    `json:"planet_name" bson:"planet_name,omitempty"`
	Stage    string    `json:"stage" bson:"stage,omitempty"`
	Status   string    `json:"status" bson:"status,omitempty"`
	CreateAt time.Time `json:"create_at" bson:"create_at,omitempty"`
}

func (p *TestDoc) DocumentID() string {
	return p.DocID
}

func NewTestStore(t *testing.T) *ESStore {
	if !localTest {
		t.Skip("skipping test in non-local environment")
	}
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

	_, err = es.Info()
	if err != nil {
		panic(err)
	}
	atomic.AddInt32(indexID, 1)
	s := &ESStore{
		IndexName: fmt.Sprintf("test_doc_%d", *indexID),
		ESClient:  es,
	}
	ESCreateIndex(context.Background(), s, "{}")
	return s
}

func Clear(s *ESStore) {
	ESDeleteIndex(context.Background(), s)
}

func TestCreate(t *testing.T) {
	s := NewTestStore(t)
	defer Clear(s)

	err := ESCreate(context.Background(),
		s,
		&TestDoc{
			DocID:    "111",
			Name:     "Earth",
			Stage:    "beta",
			Status:   "active",
			CreateAt: time.Now(),
		})
	if err != nil {
		t.Error(err)
	}
	doc := &TestDoc{}
	err = ESFindOne(context.Background(),
		s, "111", doc)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(doc.CreateAt)
}

func TestSearch(t *testing.T) {
	s := NewTestStore(t)
	defer Clear(s)

	ESCreateWaitForRefresh(context.Background(),
		s,
		&TestDoc{
			DocID:  "111",
			Name:   "Earth",
			Stage:  "beta",
			Status: "active",
		})

	docs := []*TestDoc{}
	err := ESSearch(context.Background(),
		s, &esquerydsl.QueryDoc{
			And: []esquerydsl.QueryItem{
				{Field: "doc_id", Value: "111", Type: esquerydsl.Match},
			},
		}, &docs)
	if err != nil {
		t.Error(err)
	}
	if len(docs) != 1 {
		t.Error("expected 1 doc, got", len(docs))
	}

}

func TestUpdate(t *testing.T) {
	s := NewTestStore(t)
	defer Clear(s)

	err := ESUpsert(context.Background(),
		s, &TestDoc{
			DocID:  "111",
			Name:   "Earth",
			Stage:  "beta",
			Status: "inactive",
		})
	if err != nil {
		t.Error(err)
	}
}

func TestDelete(t *testing.T) {
	s := NewTestStore(t)
	defer Clear(s)

	_, err := ESDelete(context.Background(),
		s, "111")
	if err != nil {
		t.Error(err)
	}
}

func TestUpsert(t *testing.T) {
	s := NewTestStore(t)
	defer Clear(s)

	err := ESUpsert(context.Background(),
		s, &TestDoc{
			DocID:  "111",
			Name:   "Earth",
			Stage:  "beta",
			Status: "inactive",
		})
	if err != nil {
		t.Error(err)
	}
}
