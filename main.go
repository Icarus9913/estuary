package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.opencensus.io/stats/view"

	"github.com/application-research/estuary/build"
	"github.com/application-research/estuary/config"
	drpc "github.com/application-research/estuary/drpc"
	"github.com/application-research/estuary/metrics"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/pinner"
	"github.com/application-research/estuary/stagingbs"
	"github.com/application-research/estuary/util"
	"github.com/application-research/estuary/util/gateway"
	"github.com/application-research/filclient"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/engine/chunker"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	routed "github.com/libp2p/go-libp2p/p2p/host/routed"
	"github.com/multiformats/go-multiaddr"
	"github.com/whyrusleeping/memo"
	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/lotus/api"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	cli "github.com/urfave/cli/v2"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func init() {
	if os.Getenv("FULLNODE_API_INFO") == "" {
		os.Setenv("FULLNODE_API_INFO", "wss://api.chain.love")
	}
}

var log = logging.Logger("estuary")

type storageMiner struct {
	gorm.Model
	Address         util.DbAddr `gorm:"unique"`
	Suspended       bool
	SuspendedReason string
	Name            string
	Version         string
	Location        string
	Owner           uint
}

type Content struct {
	ID        uint           `gorm:"primarykey" json:"id"`
	CreatedAt time.Time      `json:"-"`
	UpdatedAt time.Time      `json:"updatedAt"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`

	Cid         util.DbCID       `json:"cid"`
	Name        string           `json:"name"`
	UserID      uint             `json:"userId" gorm:"index"`
	Description string           `json:"description"`
	Size        int64            `json:"size"`
	Type        util.ContentType `json:"type"`
	Active      bool             `json:"active"`
	Offloaded   bool             `json:"offloaded"`
	Replication int              `json:"replication"`

	// TODO: shift most of the 'state' booleans in here into a single state
	// field, should make reasoning about things much simpler
	AggregatedIn uint `json:"aggregatedIn" gorm:"index:,option:CONCURRENTLY"`
	Aggregate    bool `json:"aggregate"`

	Pinning bool   `json:"pinning"`
	PinMeta string `json:"pinMeta"`

	Failed bool `json:"failed"`

	Location string `json:"location"`
	// TODO: shift location tracking to just use the ID of the shuttle
	// Also move towards recording content movement intentions in the database,
	// making that process more resilient to failures
	// LocID     uint   `json:"locID"`
	// LocIntent uint   `json:"locIntent"`

	// If set, this content is part of a split dag.
	// In such a case, the 'root' content should be advertised on the dht, but
	// not have deals made for it, and the children should have deals made for
	// them (unlike with aggregates)
	DagSplit  bool `json:"dagSplit"`
	SplitFrom uint `json:"splitFrom"`
}

type Object struct {
	ID         uint       `gorm:"primarykey"`
	Cid        util.DbCID `gorm:"index"`
	Size       int
	Reads      int
	LastAccess time.Time
}

type ObjRef struct {
	ID        uint `gorm:"primarykey"`
	Content   uint `gorm:"index:,option:CONCURRENTLY"`
	Object    uint `gorm:"index:,option:CONCURRENTLY"`
	Offloaded uint
}

// TODO: move to util
func stringToPrivkey(privKeyStr string) (crypto.PrivKey, error) {
	privKeyBytes, err := crypto.ConfigDecodeKey(privKeyStr)
	if err != nil {
		return nil, err
	}

	privKey, err := crypto.UnmarshalPrivateKey(privKeyBytes)
	if err != nil {
		return nil, err
	}

	return privKey, nil
}

// TODO: move to util
func multiAddrsToString(addrs []multiaddr.Multiaddr) []string {
	var rAddrs []string
	for _, addr := range addrs {
		rAddrs = append(rAddrs, addr.String())
	}
	return rAddrs
}

// TODO: move to util
func stringToMultiAddrs(addrStr string) ([]multiaddr.Multiaddr, error) {
	var mAddrs []multiaddr.Multiaddr
	for _, addr := range strings.Split(addrStr, ",") {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, err
		}
		mAddrs = append(mAddrs, ma)
	}
	return mAddrs, nil
}

// Shamelessly stolen from https://github.com/filecoin-project/index-provider/blob/c6a250f10cacb9798675ded763f6dea722ba3734/engine/chunker/cached_chunker_test.go#L322-L336
func getMhIterator(contents []Content) (provider.MultihashIterator, error) {
	idx := index.NewMultihashSorted()
	var records []index.Record
	for i, content := range contents {
		records = append(records, index.Record{
			Cid:    content.Cid.CID,
			Offset: uint64(i + 1),
		})
	}
	err := idx.Load(records)
	if err != nil {
		return nil, err
	}
	iterator, err := provider.CarMultihashIterator(idx)
	if err != nil {
		return nil, err
	}
	return iterator, nil
}

func buildAdvertisement(h host.Host, newContents []Content, contextID []byte) (schema.Advertisement, error) {
	// TODO: maybe add metadata here? currently empty
	md := metadata.New(metadata.Bitswap{})
	mdBytes, err := md.MarshalBinary()
	if err != nil {
		return schema.Advertisement{}, err
	}

	//TODO: these values are completely arbitrary
	chunkSize := 1000
	capacity := 1000
	entriesChuncker, err := chunker.NewCachedEntriesChunker(context.Background(), datastore.NewMapDatastore(), chunkSize, capacity)
	if err != nil {
		return schema.Advertisement{}, err
	}

	mhIterator, err := getMhIterator(newContents)
	if err != nil {
		return schema.Advertisement{}, err
	}

	entries, err := entriesChuncker.Chunk(context.Background(), mhIterator)
	if err != nil {
		return schema.Advertisement{}, err
	}

	ad := schema.Advertisement{
		Provider:  h.ID().String(),
		Addresses: multiAddrsToString(h.Addrs()),
		Entries:   entries,
		ContextID: contextID,
		Metadata:  mdBytes,
	}

	return ad, nil
}

// createFakeAutoretrieveHost builds a libp2p host object with the
// private key of the autoretrieve server so we can send advertisements
// on behalf of those servers (as if we were them)
// Note: this is a hack, we should create tokens that give partial
// permissions to other players
func createFakeAutoretrieveHost(ar Autoretrieve) (host.Host, error) {
	arPrivKey, err := stringToPrivkey(ar.PrivateKey)
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New(libp2p.Identity(arPrivKey))
	if err != nil {
		return nil, err
	}
	return h, nil
}

// announceNewCIDs publishes an announcement with the CIDs that were added
// between now and lastTickTime (see updateAutoretrieveIndex)
func (s *Server) announceNewCIDs(newContents []Content, ar Autoretrieve) error {
	if len(newContents) == 0 {
		return fmt.Errorf("no new CIDs to announce")
	}

	// create new host to pretend to be the autoretrieve server publishing the announcement
	h, err := createFakeAutoretrieveHost(ar)
	if err != nil {
		return err
	}

	addrs, err := stringToMultiAddrs(ar.Addresses)
	if err != nil {
		return err
	}

	// For local testing (specify pubsub on localhost)
	topic := "testingTopic"

	subHost, err := libp2p.New()
	pubHost, err := libp2p.New()
	pubG, _ := pubsub.NewGossipSub(context.Background(), pubHost,
		pubsub.WithDirectConnectTicks(1),
		pubsub.WithDirectPeers([]peer.AddrInfo{subHost.Peerstore().PeerInfo(subHost.ID())}),
	)
	pubT, err := pubG.Join(topic)

	e, err := engine.New(
		engine.WithHost(pubHost),    // TODO: remove, testing
		engine.WithTopic(pubT),      // TODO: remove, testing
		engine.WithTopicName(topic), // TODO: remove, testing
		engine.WithHost(h),
		engine.WithPublisherKind(engine.DataTransferPublisher),
		// we need these addresses to be here instead
		// of on the p2p host h because if we add them
		// as ListenAddrs it'll try to start listening locally
		engine.WithRetrievalAddrs(addrs...),
	)
	if err != nil {
		return err
	}

	e.Start(context.Background())
	defer e.Shutdown()

	// build contextID for advertisement (format: EstuaryAd-1, EstuaryAd-2, ...)
	strAdIdx := strconv.Itoa(s.IdxCtxID)
	contextID := []byte("EstuaryAd-" + strAdIdx)

	ad, err := buildAdvertisement(h, newContents, contextID)
	if err != nil {
		return err
	}

	adCID, err := e.Publish(context.Background(), ad)
	if err != nil {
		return err
	}
	log.Infof("Published advertisement: %+v", adCID)

	return nil
}

// updateAutoretrieveIndex ticks every tickInterval and checks for new CIDs added
// It updates storetheindex with the new CIDs, saying they are present on autoretrieve
// With that, clients using bitswap can query autoretrieve servers using bitswap and get data from estuary
func (s *Server) updateAutoretrieveIndex(tickInterval time.Duration, quit chan struct{}) error {
	var autoretrieves []Autoretrieve
	var newContents []Content
	var lastTickTime time.Time
	ticker := time.NewTicker(tickInterval)

	defer ticker.Stop()
	for {
		lastTickTime = time.Now().UTC().Add(-tickInterval)

		// Find all autoretrieve servers that are online (that sent heartbeat)
		err := s.DB.Find(&autoretrieves, "last_connection > ?", lastTickTime).Error
		if err != nil {
			log.Errorf("unable to query autoretrieve servers from database: %s", err)
			return err
		}
		if len(autoretrieves) > 0 {
			// err := s.DB.Find(&newContents, "updated_at > ?", lastTickTime).Group("cid").Error TODO: FIX
			err := s.DB.Find(&newContents).Error
			if err != nil {
				log.Errorf("unable to query list of new CIDs: %s", err)
				return err
			}
			if len(newContents) > 0 {
				log.Infof("announcing %d new CIDs to %d autoretrieve servers", len(newContents), len(autoretrieves))
				for _, ar := range autoretrieves {
					// send announcement with new CIDs for each autoretrieve server
					s.announceNewCIDs(newContents, ar)
					//TODO: remove old CIDs (do we even need that?)
				}
			} else {
				log.Infof("no new CIDs to advertise")
			}
		} else {
			log.Infof("no autoretrieve servers online")
		}

		// wait for next tick, or quit
		select {
		case <-ticker.C:
			continue
		case <-quit:
			break
		}
	}
}

func main() {
	logging.SetLogLevel("dt-impl", "debug")
	logging.SetLogLevel("estuary", "debug")
	logging.SetLogLevel("paych", "debug")
	logging.SetLogLevel("filclient", "debug")
	logging.SetLogLevel("dt_graphsync", "debug")
	//logging.SetLogLevel("graphsync_allocator", "debug")
	logging.SetLogLevel("dt-chanmon", "debug")
	logging.SetLogLevel("markets", "debug")
	logging.SetLogLevel("data_transfer_network", "debug")
	logging.SetLogLevel("rpc", "info")
	logging.SetLogLevel("bs-wal", "info")
	logging.SetLogLevel("provider.batched", "info")
	logging.SetLogLevel("bs-migrate", "info")

	app := cli.NewApp()
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus",
		},
		&cli.StringFlag{
			Name:    "database",
			Usage:   "specify connection string for estuary database",
			Value:   build.DefaultDatabaseValue,
			EnvVars: []string{"ESTUARY_DATABASE"},
		},
		&cli.StringFlag{
			Name:    "apilisten",
			Usage:   "address for the api server to listen on",
			Value:   ":3004",
			EnvVars: []string{"ESTUARY_API_LISTEN"},
		},
		&cli.StringFlag{
			Name:    "datadir",
			Usage:   "directory to store data in",
			Value:   ".",
			EnvVars: []string{"ESTUARY_DATADIR"},
		},
		&cli.StringFlag{
			Name:   "write-log",
			Usage:  "enable write log blockstore in specified directory",
			Hidden: true,
		},
		&cli.BoolFlag{
			Name:  "no-storage-cron",
			Usage: "run estuary without processing files into deals",
		},
		&cli.BoolFlag{
			Name:  "logging",
			Usage: "enable api endpoint logging",
		},
		&cli.BoolFlag{
			Name:   "enable-auto-retrieve",
			Hidden: true,
		},
		&cli.StringFlag{
			Name:    "lightstep-token",
			Usage:   "specify lightstep access token for enabling trace exports",
			EnvVars: []string{"ESTUARY_LIGHTSTEP_TOKEN"},
		},
		&cli.StringFlag{
			Name:  "hostname",
			Usage: "specify hostname this node will be reachable at",
			Value: "http://localhost:3004",
		},
		&cli.BoolFlag{
			Name:  "fail-deals-on-transfer-failure",
			Usage: "consider deals failed when the transfer to the miner fails",
		},
		&cli.BoolFlag{
			Name:  "disable-deal-making",
			Usage: "do not create any new deals (existing deals will still be processed)",
		},
		&cli.BoolFlag{
			Name:  "disable-content-adding",
			Usage: "disallow new content ingestion globally",
		},
		&cli.BoolFlag{
			Name:  "disable-local-content-adding",
			Usage: "disallow new content ingestion on this node (shuttles are unaffected)",
		},
		&cli.StringFlag{
			Name:  "blockstore",
			Usage: "specify blockstore parameters",
		},
		&cli.BoolFlag{
			Name: "write-log-truncate",
		},
		&cli.IntFlag{
			Name:  "default-replication",
			Value: 6,
		},
		&cli.BoolFlag{
			Name:  "lowmem",
			Usage: "TEMP: turns down certain parameters to attempt to use less memory (will be replaced by a more specific flag later)",
		},
		&cli.BoolFlag{
			Name:  "jaeger-tracing",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "jaeger-provider-url",
			Value: "http://localhost:14268/api/traces",
		},
		&cli.Float64Flag{
			Name:  "jaeger-sampler-ratio",
			Usage: "If less than 1 probabilistic metrics will be used.",
			Value: 1,
		},
	}
	app.Commands = []*cli.Command{
		{
			Name:  "setup",
			Usage: "Creates an initial auth token under new user \"admin\"",
			Action: func(cctx *cli.Context) error {
				db, err := setupDatabase(cctx)
				if err != nil {
					return err
				}

				quietdb := db.Session(&gorm.Session{
					Logger: logger.Discard,
				})

				username := "admin"
				passHash := ""

				if err := quietdb.First(&User{}, "username = ?", username).Error; err == nil {
					return fmt.Errorf("an admin user already exists")
				}

				newUser := &User{
					UUID:     uuid.New().String(),
					Username: username,
					PassHash: passHash,
					Perm:     100,
				}
				if err := db.Create(newUser).Error; err != nil {
					return fmt.Errorf("admin user creation failed: %w", err)
				}

				authToken := &AuthToken{
					Token:  "EST" + uuid.New().String() + "ARY",
					User:   newUser.ID,
					Expiry: time.Now().Add(time.Hour * 24 * 365),
				}
				if err := db.Create(authToken).Error; err != nil {
					return fmt.Errorf("admin token creation failed: %w", err)
				}

				fmt.Printf("Auth Token: %v\n", authToken.Token)

				return nil
			},
		},
	}
	app.Action = func(cctx *cli.Context) error {
		ddir := cctx.String("datadir")

		bstore := filepath.Join(ddir, "estuary-blocks")
		if bs := cctx.String("blockstore"); bs != "" {
			bstore = bs
		}
		cfg := &config.Config{
			ListenAddrs: []string{
				"/ip4/0.0.0.0/tcp/6744",
			},
			Blockstore:       bstore,
			Libp2pKeyFile:    filepath.Join(ddir, "estuary-peer.key"),
			Datastore:        filepath.Join(ddir, "estuary-leveldb"),
			WalletDir:        filepath.Join(ddir, "estuary-wallet"),
			WriteLogTruncate: cctx.Bool("write-log-truncate"),
			NoLimiter:        true,
		}

		if wl := cctx.String("write-log"); wl != "" {
			if wl[0] == '/' {
				cfg.WriteLog = wl
			} else {
				cfg.WriteLog = filepath.Join(ddir, wl)
			}
		}

		db, err := setupDatabase(cctx)
		if err != nil {
			return err
		}

		defaultReplication = cctx.Int("default-replication")

		init := Initializer{cfg, db, nil}

		nd, err := node.Setup(context.Background(), &init)
		if err != nil {
			return err
		}

		if err = view.Register(
			metrics.DefaultViews...,
		); err != nil {
			log.Fatalf("Cannot register the OpenCensus view: %v", err)
			return err
		}

		addr, err := nd.Wallet.GetDefault()
		if err != nil {
			return err
		}

		sbmgr, err := stagingbs.NewStagingBSMgr(filepath.Join(ddir, "stagingdata"))
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPI(cctx)
		// api, closer, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		// setup tracing to jaeger if enabled
		if cctx.Bool("jaeger-tracing") {
			tp, err := metrics.NewJaegerTraceProvider("estuary",
				cctx.String("jaeger-provider-url"), cctx.Float64("jaeger-sampler-ratio"))
			if err != nil {
				return err
			}
			otel.SetTracerProvider(tp)
		}

		s := &Server{
			Node:        nd,
			Api:         api,
			StagingMgr:  sbmgr,
			tracer:      otel.Tracer("api"),
			cacher:      memo.NewCacher(),
			gwayHandler: gateway.NewGatewayHandler(nd.Blockstore),
			IdxCtxID:    0,
		}

		// TODO: this is an ugly self referential hack... should fix
		pinmgr := pinner.NewPinManager(s.doPinning, nil, &pinner.PinManagerOpts{
			MaxActivePerUser: 20,
		})

		go pinmgr.Run(50)

		rhost := routed.Wrap(nd.Host, nd.FilDht)

		var opts []func(*filclient.Config)
		if cctx.Bool("lowmem") {
			opts = append(opts, func(cfg *filclient.Config) {
				cfg.GraphsyncOpts = []gsimpl.Option{
					gsimpl.MaxInProgressIncomingRequests(100),
					gsimpl.MaxInProgressOutgoingRequests(100),
					gsimpl.MaxMemoryResponder(4 << 30),
					gsimpl.MaxMemoryPerPeerResponder(16 << 20),
					gsimpl.MaxInProgressIncomingRequestsPerPeer(10),
					gsimpl.MessageSendRetries(2),
					gsimpl.SendMessageTimeout(2 * time.Minute),
				}
			})
		}

		fc, err := filclient.NewClient(rhost, api, nd.Wallet, addr, nd.Blockstore, nd.Datastore, ddir)
		if err != nil {
			return err
		}

		s.FilClient = fc

		for _, a := range nd.Host.Addrs() {
			fmt.Printf("%s/p2p/%s\n", a, nd.Host.ID())
		}

		go func() {
			for _, ai := range node.BootstrapPeers {
				if err := nd.Host.Connect(context.TODO(), ai); err != nil {
					fmt.Println("failed to connect to bootstrapper: ", err)
					continue
				}
			}

			if err := nd.Dht.Bootstrap(context.TODO()); err != nil {
				fmt.Println("dht bootstrapping failed: ", err)
			}
		}()

		s.DB = db

		cm, err := NewContentManager(db, api, fc, init.trackingBstore, s.Node.NotifBlockstore, nd.Provider, pinmgr, nd, cctx.String("hostname"))
		if err != nil {
			return err
		}

		fc.SetPieceCommFunc(cm.getPieceCommitment)

		cm.FailDealOnTransferFailure = cctx.Bool("fail-deals-on-transfer-failure")

		cm.isDealMakingDisabled = cctx.Bool("disable-deal-making")
		cm.contentAddingDisabled = cctx.Bool("disable-content-adding")
		cm.localContentAddingDisabled = cctx.Bool("disable-local-content-adding")

		cm.tracer = otel.Tracer("replicator")

		if cctx.Bool("enable-auto-retrive") {
			init.trackingBstore.SetCidReqFunc(cm.RefreshContentForCid)
		}

		if !cctx.Bool("no-storage-cron") {
			go cm.ContentWatcher()
		}

		s.CM = cm

		if !cm.contentAddingDisabled {
			go func() {
				// wait for shuttles to reconnect
				// This is a bit of a hack, and theres probably a better way to
				// solve this. but its good enough for now
				time.Sleep(time.Second * 10)

				if err := cm.refreshPinQueue(); err != nil {
					log.Errorf("failed to refresh pin queue: %s", err)
				}
			}()
		}

		// start autoretrieve index updater task every INDEX_UPDATE_INTERVAL minutes

		updateInterval, ok := os.LookupEnv("INDEX_UPDATE_INTERVAL")
		if !ok {
			updateInterval = "720"
		}
		intervalMinutes, err := strconv.Atoi(updateInterval)
		if err != nil {
			return err
		}

		stopUpdateIndex := make(chan struct{})
		go s.updateAutoretrieveIndex(time.Duration(intervalMinutes)*time.Second, stopUpdateIndex) //TODO: minute

		go func() {
			time.Sleep(time.Second * 10)

			if err := s.RestartAllTransfersForLocation(context.TODO(), "local"); err != nil {
				log.Errorf("failed to restart transfers: %s", err)
			}
		}()

		return s.ServeAPI(cctx.String("apilisten"), cctx.Bool("logging"), cctx.String("lightstep-token"), filepath.Join(ddir, "cache"))
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
	}
}

type Autoretrieve struct {
	gorm.Model

	Handle         string `gorm:"unique"`
	Token          string `gorm:"unique"`
	LastConnection time.Time
	PrivateKey     string `gorm:"unique"`
	Addresses      string
}

func setupDatabase(cctx *cli.Context) (*gorm.DB, error) {
	dbval := cctx.String("database")

	/* TODO: change this default
	ddir := cctx.String("datadir")
	if dbval == defaultDatabaseValue && ddir != "." {
		dbval = "sqlite=" + filepath.Join(ddir, "estuary.db")
	}
	*/

	db, err := util.SetupDatabase(dbval)
	if err != nil {
		return nil, err
	}

	db.AutoMigrate(&Content{})
	db.AutoMigrate(&Object{})
	db.AutoMigrate(&ObjRef{})
	db.AutoMigrate(&Collection{})
	db.AutoMigrate(&CollectionRef{})

	db.AutoMigrate(&contentDeal{})
	db.AutoMigrate(&dfeRecord{})
	db.AutoMigrate(&PieceCommRecord{})
	db.AutoMigrate(&proposalRecord{})
	db.AutoMigrate(&util.RetrievalFailureRecord{})
	db.AutoMigrate(&retrievalSuccessRecord{})

	db.AutoMigrate(&minerStorageAsk{})
	db.AutoMigrate(&storageMiner{})

	db.AutoMigrate(&User{})
	db.AutoMigrate(&AuthToken{})
	db.AutoMigrate(&InviteCode{})

	db.AutoMigrate(&Shuttle{})

	db.AutoMigrate(&Autoretrieve{})

	// 'manually' add unique composite index on collection fields because gorms syntax for it is tricky
	if err := db.Exec("create unique index if not exists collection_refs_paths on collection_refs (path,collection)").Error; err != nil {
		return nil, fmt.Errorf("failed to create collection paths index: %w", err)
	}

	var count int64
	if err := db.Model(&storageMiner{}).Count(&count).Error; err != nil {
		return nil, err
	}

	if count == 0 {
		fmt.Println("adding default miner list to database...")
		for _, m := range build.DefaultMiners {
			db.Create(&storageMiner{Address: util.DbAddr{m}})
		}

	}
	return db, nil
}

type Server struct {
	tracer     trace.Tracer
	Node       *node.Node
	DB         *gorm.DB
	FilClient  *filclient.FilClient
	Api        api.Gateway
	CM         *ContentManager
	StagingMgr *stagingbs.StagingBSMgr
	IdxCtxID   int

	gwayHandler *gateway.GatewayHandler

	cacher *memo.Cacher
}

func (s *Server) GarbageCollect(ctx context.Context) error {
	// since we're reference counting all the content, garbage collection becomes easy
	// its even easier if we don't care that its 'perfect'

	// We can probably even just remove stuff when its references are removed from the database
	keych, err := s.Node.Blockstore.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	for c := range keych {
		keep, err := s.trackingObject(c)
		if err != nil {
			return err
		}

		if !keep {
			// can batch these deletes and execute them at the datastore layer for more perfs
			if err := s.Node.Blockstore.DeleteBlock(ctx, c); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Server) trackingObject(c cid.Cid) (bool, error) {
	var count int64
	if err := s.DB.Model(&Object{}).Where("cid = ?", c.Bytes()).Count(&count).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}

	return count > 0, nil
}

func jsondump(o interface{}) {
	data, _ := json.MarshalIndent(o, "", "  ")
	fmt.Println(string(data))
}

func (s *Server) RestartAllTransfersForLocation(ctx context.Context, loc string) error {
	var deals []contentDeal
	if err := s.DB.Model(contentDeal{}).
		Joins("left join contents on contents.id = content_deals.content").
		Where("not content_deals.failed and content_deals.deal_id = 0 and content_deals.dt_chan != '' and location = ?", loc).
		Scan(&deals).Error; err != nil {
		return err
	}

	for _, d := range deals {
		chid, err := d.ChannelID()
		if err != nil {
			log.Errorf("failed to get channel id from deal %d: %s", d.ID, err)
			continue
		}

		if err := s.CM.RestartTransfer(ctx, loc, chid); err != nil {
			log.Errorf("failed to restart transfer: %s", err)
			continue
		}
	}

	return nil
}

func (cm *ContentManager) RestartTransfer(ctx context.Context, loc string, chanid datatransfer.ChannelID) error {
	if loc == "local" {
		st, err := cm.FilClient.TransferStatus(ctx, &chanid)
		if err != nil {
			return err
		}

		if util.TransferTerminated(st) {
			return fmt.Errorf("deal in database as being in progress, but data transfer is terminated: %d", st.Status)
		}

		return cm.FilClient.RestartTransfer(ctx, &chanid)
	}

	return cm.sendRestartTransferCmd(ctx, loc, chanid)
}

func (cm *ContentManager) sendRestartTransferCmd(ctx context.Context, loc string, chanid datatransfer.ChannelID) error {
	return cm.sendShuttleCommand(ctx, loc, &drpc.Command{
		Op: drpc.CMD_RestartTransfer,
		Params: drpc.CmdParams{
			RestartTransfer: &drpc.RestartTransfer{
				ChanID: chanid,
			},
		},
	})
}
