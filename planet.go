package main

import "time"

type Planet struct {
	PlanetID string   `json:"planet_id" bson:"planet_id,omitempty"`
	Name     string   `json:"planet_name" bson:"planet_name,omitempty"`
	Stage    string   `json:"stage" bson:"stage,omitempty"`
	Status   string   `json:"status" bson:"status,omitempty"`
	BanInfo  *BanInfo `json:"ban_info" bson:"ban_info,omitempty"`
}

func (p *Planet) DocumentID() string {
	return p.PlanetID
}

type BanInfo struct {
	BeginTime time.Time `json:"begin_time" bson:"begin_time"`
	EndTime   time.Time `json:"end_time" bson:"end_time"`
	Reason    string    `json:"reason" bson:"reason"`
}
