package query

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/store"

	"sort"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var testGRPCOpts = []grpc.DialOption{
	grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
	grpc.WithInsecure(),
}

type testStore struct {
	info storepb.InfoResponse
}

func (s *testStore) Info(ctx context.Context, r *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	return &s.info, nil
}

func (s *testStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	return status.Error(codes.Unimplemented, "not implemented")
}

func (s *testStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *testStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

type testStoreMeta struct {
	extlsetFn func(addr string) []storepb.LabelSet
	storeType component.StoreAPI
}

type testStores struct {
	srvs map[string]*grpc.Server
}

func startTestStores(stores []testStoreMeta) (*testStores, error) {
	st := &testStores{
		srvs: map[string]*grpc.Server{},
	}

	for _, store := range stores {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			// Close so far started servers.
			st.Close()
			return nil, err
		}

		srv := grpc.NewServer()
		storepb.RegisterStoreServer(srv, &testStore{info: storepb.InfoResponse{LabelSets: store.extlsetFn(listener.Addr().String()), StoreType: store.storeType.ToProto()}})
		go func() {
			_ = srv.Serve(listener)
		}()

		st.srvs[listener.Addr().String()] = srv
	}

	return st, nil
}

func (s *testStores) StoreAddresses() []string {
	var stores []string
	for addr := range s.srvs {
		stores = append(stores, addr)
	}
	return stores
}

func (s *testStores) Close() {
	for _, srv := range s.srvs {
		srv.Stop()
	}
	s.srvs = nil
}

func (s *testStores) CloseOne(addr string) {
	srv, ok := s.srvs[addr]
	if !ok {
		return
	}

	srv.Stop()
	delete(s.srvs, addr)
}

func specsFromAddrFunc(addrs []string) func() []StoreSpec {
	return func() (specs []StoreSpec) {
		for _, addr := range addrs {
			specs = append(specs, NewGRPCStoreSpec(addr))
		}
		return specs
	}
}

func TestStoreSet_AllAvailable_ThenDown(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	st, err := startTestStores([]testStoreMeta{
		{
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{
								Name:  "addr",
								Value: addr,
							},
						},
					},
				}
			},
			storeType: component.Sidecar,
		},
		{
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{
								Name:  "addr",
								Value: addr,
							},
						},
					},
				}
			},
			storeType: component.Sidecar,
		},
	})
	testutil.Ok(t, err)
	defer st.Close()

	initialStoreAddr := st.StoreAddresses()

	// Testing if duplicates can cause weird results.
	initialStoreAddr = append(initialStoreAddr, initialStoreAddr[0])
	storeSet := NewStoreSet(nil, nil, specsFromAddrFunc(initialStoreAddr), testGRPCOpts, time.Minute)
	storeSet.gRPCInfoCallTimeout = 2 * time.Second
	defer storeSet.Close()

	// Should not matter how many of these we run.
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())

	testutil.Assert(t, len(storeSet.stores) == 2, "all services should respond just fine, so we expect all clients to be ready.")

	for addr, store := range storeSet.stores {
		testutil.Equals(t, addr, store.addr)
		testutil.Equals(t, 1, len(store.labelSets))
		testutil.Equals(t, "addr", store.labelSets[0].Labels[0].Name)
		testutil.Equals(t, addr, store.labelSets[0].Labels[0].Value)
	}

	st.CloseOne(initialStoreAddr[0])

	// We expect Update to tear down store client for closed store server.
	storeSet.Update(context.Background())

	testutil.Assert(t, len(storeSet.stores) == 1, "only one service should respond just fine, so we expect one client to be ready.")

	addr := initialStoreAddr[1]
	store, ok := storeSet.stores[addr]
	testutil.Assert(t, ok, "addr exist")
	testutil.Equals(t, addr, store.addr)
	testutil.Equals(t, 1, len(store.labelSets))
	testutil.Equals(t, "addr", store.labelSets[0].Labels[0].Name)
	testutil.Equals(t, addr, store.labelSets[0].Labels[0].Value)
}

func TestStoreSet_StaticStores_OneAvailable(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	st, err := startTestStores([]testStoreMeta{
		{
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{
								Name:  "addr",
								Value: addr,
							},
						},
					},
				}
			},
			storeType: component.Sidecar,
		},
		{
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{
								Name:  "addr",
								Value: addr,
							},
						},
					},
				}
			},
			storeType: component.Sidecar,
		},
	})
	testutil.Ok(t, err)
	defer st.Close()

	initialStoreAddr := st.StoreAddresses()
	st.CloseOne(initialStoreAddr[0])

	storeSet := NewStoreSet(nil, nil, specsFromAddrFunc(initialStoreAddr), testGRPCOpts, time.Minute)
	storeSet.gRPCInfoCallTimeout = 2 * time.Second
	defer storeSet.Close()

	// Should not matter how many of these we run.
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())

	testutil.Assert(t, len(storeSet.stores) == 1, "only one service should respond just fine, so we expect one client to be ready.")

	addr := initialStoreAddr[1]
	store, ok := storeSet.stores[addr]
	testutil.Assert(t, ok, "addr exist")
	testutil.Equals(t, addr, store.addr)
	testutil.Equals(t, 1, len(store.labelSets))
	testutil.Equals(t, "addr", store.labelSets[0].Labels[0].Name)
	testutil.Equals(t, addr, store.labelSets[0].Labels[0].Value)
}

func TestStoreSet_StaticStores_NoneAvailable(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	st, err := startTestStores([]testStoreMeta{
		{
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{
								Name:  "addr",
								Value: addr,
							},
						},
					},
				}
			},
			storeType: component.Sidecar,
		},
		{
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{
								Name:  "addr",
								Value: addr,
							},
						},
					},
				}
			},
			storeType: component.Sidecar,
		},
	})
	testutil.Ok(t, err)
	defer st.Close()

	initialStoreAddr := st.StoreAddresses()
	st.CloseOne(initialStoreAddr[0])
	st.CloseOne(initialStoreAddr[1])

	storeSet := NewStoreSet(nil, nil, specsFromAddrFunc(initialStoreAddr), testGRPCOpts, time.Minute)
	storeSet.gRPCInfoCallTimeout = 2 * time.Second

	// Should not matter how many of these we run.
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())
	testutil.Assert(t, len(storeSet.stores) == 0, "none of services should respond just fine, so we expect no client to be ready.")

	// Leak test will ensure that we don't keep client connection around.

}

// TODO(bwplotka): Bring back duplicate check https://github.com/thanos-io/thanos/issues/1635
// For now allow all.
func TestStoreSet_AllAvailable_BlockExtLsetDuplicates(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	st, err := startTestStores([]testStoreMeta{
		{
			storeType: component.Query,
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
					{
						Labels: []storepb.Label{
							{Name: "l3", Value: "v4"},
						},
					},
				}
			},
		},
		{
			// Duplicated Querier.
			storeType: component.Query,
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
					{
						Labels: []storepb.Label{
							{Name: "l3", Value: "v4"},
						},
					},
				}
			},
		},
		{
			storeType: component.Sidecar,
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
				}
			},
		},
		{
			// Duplicated Sidecar.
			storeType: component.Sidecar,
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
				}
			},
		},
		{
			// Querier that duplicates with sidecar.
			storeType: component.Query,
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
				}
			},
		},
		{
			storeType: component.Rule,
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
				}
			},
		},
		{
			// Duplicated Rule.
			storeType: component.Rule,
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
				}
			},
		},
		// Two pre v0.8.0 store gateway nodes, they don't have ext labels set.
		{
			storeType: component.Store,
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{}
			},
		},
		{
			storeType: component.Store,
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{}
			},
		},
		// Regression tests against https://github.com/thanos-io/thanos/issues/1632: From v0.8.0 stores advertise labels.
		// If the object storage handled by store gateway has only one sidecar we were hitting issue.
		{
			storeType: component.Store,
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
					{
						Labels: []storepb.Label{
							{Name: "l3", Value: "v4"},
						},
					},
				}
			},
		},
		// Stores v0.8.1 has compatibility labels. Check if they are correctly removed.
		{
			storeType: component.Store,
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
					{
						Labels: []storepb.Label{
							{Name: "l3", Value: "v4"},
						},
					},
					{
						Labels: []storepb.Label{
							{Name: store.CompatibilityTypeLabelName, Value: "store"},
						},
					},
				}
			},
		},
		{
			storeType: component.Store,
			extlsetFn: func(addr string) []storepb.LabelSet {
				return []storepb.LabelSet{
					{
						Labels: []storepb.Label{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
					{
						Labels: []storepb.Label{
							{Name: "l3", Value: "v4"},
						},
					},
					{
						Labels: []storepb.Label{
							{Name: store.CompatibilityTypeLabelName, Value: "store"},
						},
					},
				}
			},
		},
	})
	testutil.Ok(t, err)
	defer st.Close()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, level.AllowDebug())
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	storeSet := NewStoreSet(logger, nil, specsFromAddrFunc(st.StoreAddresses()), testGRPCOpts, time.Minute)
	storeSet.gRPCInfoCallTimeout = 2 * time.Second
	defer storeSet.Close()

	// Should not matter how many of these we run.
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())

	testutil.Assert(t, len(storeSet.stores) == 12, fmt.Sprintf("all services should respond just fine, but we expect duplicates being blocked. Expected %d stores, got %d", 12, len(storeSet.stores)))

	// Sort result to be able to compare.
	var existingStoreLabels [][][]storepb.Label
	for _, store := range storeSet.stores {
		lset := [][]storepb.Label{}
		for _, ls := range store.LabelSets() {
			lset = append(lset, ls.Labels)
		}
		existingStoreLabels = append(existingStoreLabels, lset)
	}
	sort.Slice(existingStoreLabels, func(i, j int) bool {
		return len(existingStoreLabels[i]) > len(existingStoreLabels[j])
	})

	testutil.Equals(t, [][][]storepb.Label{
		{
			{
				{Name: "l1", Value: "v2"},
				{Name: "l2", Value: "v3"},
			},
			{
				{Name: "l3", Value: "v4"},
			},
		},
		{
			{
				{Name: "l1", Value: "v2"},
				{Name: "l2", Value: "v3"},
			},
			{
				{Name: "l3", Value: "v4"},
			},
		},
		{
			{
				{Name: "l1", Value: "v2"},
				{Name: "l2", Value: "v3"},
			},
		},
		{
			{
				{Name: "l1", Value: "v2"},
				{Name: "l2", Value: "v3"},
			},
		},
		{
			{
				{Name: "l1", Value: "v2"},
				{Name: "l2", Value: "v3"},
			},
		},
		{
			{
				{Name: "l1", Value: "v2"},
				{Name: "l2", Value: "v3"},
			},
		},
		{
			{
				{Name: "l1", Value: "v2"},
				{Name: "l2", Value: "v3"},
			},
		},
		{},
		{},
		{
			{
				{Name: "l1", Value: "v2"},
				{Name: "l2", Value: "v3"},
			},
			{
				{Name: "l3", Value: "v4"},
			},
		},
		{
			{
				{Name: "l1", Value: "v2"},
				{Name: "l2", Value: "v3"},
			},
			{
				{Name: "l3", Value: "v4"},
			},
		},
		{
			{
				{Name: "l1", Value: "v2"},
				{Name: "l2", Value: "v3"},
			},
			{
				{Name: "l3", Value: "v4"},
			},
		},
	}, existingStoreLabels)
}
