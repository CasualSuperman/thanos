package main

import (
	"context"
	"net"
	"time"

	"github.com/thanos-io/thanos/pkg/extflag"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"
)

// registerStore registers a store command.
func registerStore(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command(component.Store.String(), "store node giving access to blocks in a bucket provider. Now supported GCS, S3, Azure, Swift and Tencent COS.")

	grpcBindAddr, httpBindAddr, cert, key, clientCA := regCommonServerFlags(cmd)

	dataDir := cmd.Flag("data-dir", "Data directory in which to cache remote blocks.").
		Default("./data").String()

	indexCacheSize := cmd.Flag("index-cache-size", "Maximum size of items held in the index cache.").
		Default("250MB").Bytes()

	chunkPoolSize := cmd.Flag("chunk-pool-size", "Maximum size of concurrently allocatable bytes for chunks.").
		Default("2GB").Bytes()

	maxSampleCount := cmd.Flag("store.grpc.series-sample-limit",
		"Maximum amount of samples returned via a single Series call. 0 means no limit. NOTE: for efficiency we take 120 as the number of samples in chunk (it cannot be bigger than that), so the actual number of samples might be lower, even though the maximum could be hit.").
		Default("0").Uint()

	maxConcurrent := cmd.Flag("store.grpc.series-max-concurrency", "Maximum number of concurrent Series calls.").Default("20").Int()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", true)

	syncInterval := cmd.Flag("sync-block-duration", "Repeat interval for syncing the blocks between local and remote view.").
		Default("3m").Duration()

	blockSyncConcurrency := cmd.Flag("block-sync-concurrency", "Number of goroutines to use when syncing blocks from object storage.").
		Default("20").Int()

	minTime := model.TimeOrDuration(cmd.Flag("min-time", "Start of time range limit to serve. Thanos Store will serve only metrics, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z"))

	maxTime := model.TimeOrDuration(cmd.Flag("max-time", "End of time range limit to serve. Thanos Store will serve only blocks, which happened eariler than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("9999-12-31T23:59:59Z"))

	selectorRelabelConf := regSelectorRelabelFlags(cmd)

	m[component.Store.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, debugLogging bool) error {
		if minTime.PrometheusTimestamp() > maxTime.PrometheusTimestamp() {
			return errors.Errorf("invalid argument: --min-time '%s' can't be greater than --max-time '%s'",
				minTime, maxTime)
		}

		return runStore(g,
			logger,
			reg,
			tracer,
			objStoreConfig,
			*dataDir,
			*grpcBindAddr,
			*cert,
			*key,
			*clientCA,
			*httpBindAddr,
			uint64(*indexCacheSize),
			uint64(*chunkPoolSize),
			uint64(*maxSampleCount),
			int(*maxConcurrent),
			component.Store,
			debugLogging,
			*syncInterval,
			*blockSyncConcurrency,
			&store.FilterConfig{
				MinTime: *minTime,
				MaxTime: *maxTime,
			},
			selectorRelabelConf,
		)
	}
}

// runStore starts a daemon that serves queries to cluster peers using data from an object store.
func runStore(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	objStoreConfig *extflag.PathOrContent,
	dataDir string,
	grpcBindAddr string,
	cert string,
	key string,
	clientCA string,
	httpBindAddr string,
	indexCacheSizeBytes uint64,
	chunkPoolSizeBytes uint64,
	maxSampleCount uint64,
	maxConcurrent int,
	component component.Component,
	verbose bool,
	syncInterval time.Duration,
	blockSyncConcurrency int,
	filterConf *store.FilterConfig,
	selectorRelabelConf *extflag.PathOrContent,
) error {
	statusProber := prober.NewProber(component, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))

	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	bkt, err := client.NewBucket(logger, confContentYaml, reg, component.String())
	if err != nil {
		return errors.Wrap(err, "create bucket client")
	}

	relabelContentYaml, err := selectorRelabelConf.Content()
	if err != nil {
		return errors.Wrap(err, "get content of relabel configuration")
	}

	relabelConfig, err := parseRelabelConfig(relabelContentYaml)
	if err != nil {
		return err
	}

	// Ensure we close up everything properly.
	defer func() {
		if err != nil {
			runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
		}
	}()

	// TODO(bwplotka): Add as a flag?
	maxItemSizeBytes := indexCacheSizeBytes / 2

	indexCache, err := storecache.NewIndexCache(logger, reg, storecache.Opts{
		MaxSizeBytes:     indexCacheSizeBytes,
		MaxItemSizeBytes: maxItemSizeBytes,
	})
	if err != nil {
		return errors.Wrap(err, "create index cache")
	}

	bs, err := store.NewBucketStore(
		logger,
		reg,
		bkt,
		dataDir,
		indexCache,
		chunkPoolSizeBytes,
		maxSampleCount,
		maxConcurrent,
		verbose,
		blockSyncConcurrency,
		filterConf,
		relabelConfig,
	)
	if err != nil {
		return errors.Wrap(err, "create object storage store")
	}

	begin := time.Now()
	level.Debug(logger).Log("msg", "initializing bucket store")
	if err := bs.InitialSync(context.Background()); err != nil {
		return errors.Wrap(err, "bucket store initial sync")
	}
	level.Debug(logger).Log("msg", "bucket store ready", "init_duration", time.Since(begin).String())

	ctx, cancel := context.WithCancel(context.Background())
	g.Add(func() error {
		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		err := runutil.Repeat(syncInterval, ctx.Done(), func() error {
			if err := bs.SyncBlocks(ctx); err != nil {
				level.Warn(logger).Log("msg", "syncing blocks failed", "err", err)
			}
			return nil
		})

		runutil.CloseWithLogOnErr(logger, bs, "bucket store")
		return err
	}, func(error) {
		cancel()
	})

	l, err := net.Listen("tcp", grpcBindAddr)
	if err != nil {
		return errors.Wrap(err, "listen API address")
	}

	opts, err := defaultGRPCServerOpts(logger, cert, key, clientCA)
	if err != nil {
		return errors.Wrap(err, "grpc server options")
	}
	s := newStoreGRPCServer(logger, reg, tracer, bs, opts)

	g.Add(func() error {
		level.Info(logger).Log("msg", "Listening for StoreAPI gRPC", "address", grpcBindAddr)
		statusProber.SetReady()
		return errors.Wrap(s.Serve(l), "serve gRPC")
	}, func(error) {
		s.Stop()
	})

	if err := scheduleHTTPServer(g, logger, reg, statusProber, httpBindAddr, nil, component); err != nil {
		return errors.Wrap(err, "schedule HTTP server")
	}

	level.Info(logger).Log("msg", "starting store node")
	return nil
}

func parseRelabelConfig(contentYaml []byte) ([]*relabel.Config, error) {
	var relabelConfig []*relabel.Config
	if err := yaml.Unmarshal(contentYaml, &relabelConfig); err != nil {
		return nil, errors.Wrap(err, "parsing relabel configuration")
	}

	return relabelConfig, nil
}
