package cluster

import (
    "common"
)

// A subscription implements an interface for writing and querying data.
// It can be copied to multiple servers or the local datastore.
// Subscriptions return data from between [startTm, endTm]
// Ids are the data streams being listened to
type Subscription interface {
    Id() int64
    StartTm() time.Time
    EndTm() time.Time
    Write(*p.Request) error
    SyncWrite(*p.Request) error
    Query(querySpec *parser.QuerySpec, response chan *p.Response)
}

type NewSubscription struct {
    Ids         []int
    StartTime   time.Time
    EndTime     time.Time
}

type SubscriptionData struct {
    ids             []int
    startTime       time.Time
    endTime         time.Time
    wal             WAL
    server          wal.Server
    store           LocalSubscriptionStore
    localServerId   uint32
    IsLocal         bool
}

func NewSubscription(ids []int, startTime, endTime time.Time, wal WAL) *SubscriptionData {
    return &SubscriptionData{
        ids:            ids,
        startTime:      startTime,
        endTime:        endTime,
        wal:            wal,
    }
}

var (
    queryRequest = p.Request_QUERY
)

type LocalSubscriptionStore interface {
    Write(request *p.Request) error
    SetWriteBuffer(writeBuffer *WriteBuffer)
    BufferWrite(request *p.Request)
    GetOrCreateSubscription(ids []int) (LocalSubscriptionDb, error)
    ReturnSubscription(id int)
    DeleteSubscription(id int) error
}
