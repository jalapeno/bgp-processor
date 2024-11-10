package arangodb

import (
	"context"
	"encoding/json"

	driver "github.com/arangodb/go-driver"
	"github.com/cisco-open/jalapeno/topology/dbclient"
	notifier "github.com/cisco-open/jalapeno/topology/kafkanotifier"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/tools"
)

type arangoDB struct {
	dbclient.DB
	*ArangoConn
	stop            chan struct{}
	peer            driver.Collection
	ebgpPeer        driver.Collection
	ibgpPeer        driver.Collection
	asbr            driver.Collection
	unicastprefixV4 driver.Collection
	unicastprefixV6 driver.Collection
	ebgpprefixV4    driver.Collection
	ebgpprefixV6    driver.Collection
	inetprefixV4    driver.Collection
	inetprefixV6    driver.Collection
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, peer, ebgpPeer string, ibgpPeer string, asbr string,
	unicastprefixV4, unicastprefixV6, ebgpprefixV4, ebgpprefixV6,
	inetprefixV4 string, inetprefixV6 string) (dbclient.Srv, error) {
	if err := tools.URLAddrValidation(arangoSrv); err != nil {
		return nil, err
	}
	arangoConn, err := NewArango(ArangoConfig{
		URL:      arangoSrv,
		User:     user,
		Password: pass,
		Database: dbname,
	})
	if err != nil {
		return nil, err
	}
	arango := &arangoDB{
		stop: make(chan struct{}),
	}
	arango.DB = arango
	arango.ArangoConn = arangoConn

	// Check if peer collection exists, if not fail as Jalapeno topology is not running
	arango.peer, err = arango.db.Collection(context.TODO(), peer)
	if err != nil {
		return nil, err
	}
	// Check if unicast_prefix_v4 collection exists, if not fail as Jalapeno topology is not running
	arango.unicastprefixV4, err = arango.db.Collection(context.TODO(), unicastprefixV4)
	if err != nil {
		return nil, err
	}
	// Check if unicast_prefix_v4 collection exists, if not fail as Jalapeno ipv4_topology is not running
	arango.unicastprefixV6, err = arango.db.Collection(context.TODO(), unicastprefixV6)
	if err != nil {
		return nil, err
	}

	// check for ebgp_peer collection
	found, err := arango.db.CollectionExists(context.TODO(), ebgpPeer)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), ebgpPeer)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// check for ibgp_peer collection
	found, err = arango.db.CollectionExists(context.TODO(), ibgpPeer)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), ibgpPeer)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// check for asbr collection
	found, err = arango.db.CollectionExists(context.TODO(), asbr)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), asbr)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// check for ebgp4 prefix collection
	found, err = arango.db.CollectionExists(context.TODO(), ebgpprefixV4)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), ebgpprefixV4)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}
	// check for ebgp6 prefix collection
	found, err = arango.db.CollectionExists(context.TODO(), ebgpprefixV6)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), ebgpprefixV6)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// check for inet4 prefix collection
	found, err = arango.db.CollectionExists(context.TODO(), inetprefixV4)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), inetprefixV4)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}
	// check for inet6 prefix collection
	found, err = arango.db.CollectionExists(context.TODO(), inetprefixV6)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), inetprefixV6)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// create ebgp_peer collection
	var ebgpPeer_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ebgpPeer, err = arango.db.CreateCollection(context.TODO(), "ebgp_peer", ebgpPeer_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.ebgpPeer, err = arango.db.Collection(context.TODO(), ebgpPeer)
	if err != nil {
		return nil, err
	}

	// create ibgp_peer collection
	var ibgpPeer_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ibgpPeer, err = arango.db.CreateCollection(context.TODO(), "ibgp_peer", ibgpPeer_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.ibgpPeer, err = arango.db.Collection(context.TODO(), ibgpPeer)
	if err != nil {
		return nil, err
	}

	// create asbr collection
	var asbr_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.asbr, err = arango.db.CreateCollection(context.TODO(), "asbr", asbr_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.asbr, err = arango.db.Collection(context.TODO(), asbr)
	if err != nil {
		return nil, err
	}

	// create ebgp prefix V4 collection
	var ebgpprefixV4_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ebgpprefixV4, err = arango.db.CreateCollection(context.TODO(), "ebgp_prefix_v4", ebgpprefixV4_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.ebgpprefixV4, err = arango.db.Collection(context.TODO(), ebgpprefixV4)
	if err != nil {
		return nil, err
	}

	// create ebgp prefix V6 collection
	var ebgpprefixV6_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ebgpprefixV6, err = arango.db.CreateCollection(context.TODO(), "ebgp_prefix_v6", ebgpprefixV6_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.ebgpprefixV6, err = arango.db.Collection(context.TODO(), ebgpprefixV6)
	if err != nil {
		return nil, err
	}

	// create inet prefix V4 collection
	var inetV4_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.inetprefixV4, err = arango.db.CreateCollection(context.TODO(), "inet_prefix_v4", inetV4_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.inetprefixV4, err = arango.db.Collection(context.TODO(), inetprefixV4)
	if err != nil {
		return nil, err
	}

	// create unicast prefix V6 collection
	var inetV6_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.inetprefixV6, err = arango.db.CreateCollection(context.TODO(), "inet_prefix_v6", inetV6_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.inetprefixV6, err = arango.db.Collection(context.TODO(), inetprefixV6)
	if err != nil {
		return nil, err
	}
	return arango, nil
}

func (a *arangoDB) Start() error {
	if err := a.loadCollection(); err != nil {
		return err
	}
	glog.Infof("Connected to arango database, starting monitor")
	go a.monitor()

	return nil
}

func (a *arangoDB) Stop() error {
	close(a.stop)

	return nil
}

func (a *arangoDB) GetInterface() dbclient.DB {
	return a.DB
}

func (a *arangoDB) GetArangoDBInterface() *ArangoConn {
	return a.ArangoConn
}

func (a *arangoDB) StoreMessage(msgType dbclient.CollectionType, msg []byte) error {
	event := &notifier.EventMessage{}
	if err := json.Unmarshal(msg, event); err != nil {
		return err
	}
	event.TopicType = msgType
	switch msgType {
	case bmp.PeerStateChangeMsg:
		return a.peerHandler(event)
	case bmp.UnicastPrefixV4Msg:
		return a.unicastV4Handler(event)
	case bmp.UnicastPrefixV6Msg:
		return a.unicastV6Handler(event)
	}
	return nil
}

func (a *arangoDB) monitor() {
	for {
		select {
		case <-a.stop:
			return
		}
	}
}

func (a *arangoDB) loadCollection() error {

	ctx := context.TODO()

	// Find and populate ibgp_peers
	glog.Infof("copying unique ibgp peers into ibgp_peer collection")
	ibgp_peer_query := "for l in ls_node for p in peer filter l.peer_asn == p.remote_asn " +
		"filter p.remote_asn == p.local_asn insert { _key: CONCAT_SEPARATOR(" + "\"_\", p.remote_bgp_id, p.remote_asn), " +
		"asn: p.remote_asn, name: l.name, igp_router_id: l .igp_router_id, protocol: l.protocol, area_id: l.area_id, " +
		"protocol_id: l.protocol_id  } INTO ibgp_peer OPTIONS { ignoreErrors: true }"
	cursor, err := a.db.Query(ctx, ibgp_peer_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// Find and populate ebgp_peers
	glog.Infof("copying unique ebgp peers into ebgp_peer collection")
	ebgp_peer_query := "for p in peer let internal_asns = ( for l in ls_node return l.peer_asn ) " +
		"filter p.remote_asn not in internal_asns insert { _key: CONCAT_SEPARATOR(" + "\"_\", p.remote_bgp_id, p.remote_asn), " +
		"router_id: p.remote_bgp_id, asn: p.remote_asn  } INTO ebgp_peer OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, ebgp_peer_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	glog.Infof("copying private ASN unicast v4 prefixes into non-Internet ebgp_prefix_v4 collection")
	ebgp4_query := "for l in unicast_prefix_v4 filter l.origin_as in 64512..65535 filter l.prefix_len < 26 filter l.prefix_len != null " +
		"filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", l.prefix, l.prefix_len), prefix: l.prefix, prefix_len: l.prefix_len, " +
		"origin_as: l.origin_as, nexthop: l.nexthop } INTO ebgp_prefix_v4 OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, ebgp4_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	glog.Infof("copying public ASN unicast v4 prefixes into inet_prefix_v4 collection")
	inet4_query := "for u in unicast_prefix_v4 let internal_asns = ( for l in ls_node return l.peer_asn ) " +
		"filter u.peer_asn not in internal_asns filter u.peer_asn !in 64512..65535 filter u.prefix_len < 26 " +
		"filter u.remote_asn != u.origin_as INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", u.prefix, u.prefix_len)," +
		"prefix: u.prefix, prefix_len: u.prefix_len, origin_as: u.origin_as, nexthop: u.nexthop } " +
		"INTO inet_prefix_v4 OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, inet4_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	glog.Infof("copying private ASN ebgp unicast v6 prefixes into ebgp_prefix_v6 collection")
	ebgp6_query := "for l in unicast_prefix_v6 filter l.origin_as in 64512..65535 filter l.prefix_len < 96 filter l.prefix_len != null " +
		"filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", l.prefix, l.prefix_len), prefix: l.prefix, prefix_len: l.prefix_len, " +
		"origin_as: l.origin_as, nexthop: l.nexthop } INTO ebgp_prefix_v6 OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, ebgp6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	glog.Infof("copying public ASN unicast v6 prefixes into inet_prefix_v6 collection")
	inet6_query := "for u in unicast_prefix_v6 let internal_asns = ( for l in ls_node return l.peer_asn ) " +
		"filter u.peer_asn not in internal_asns filter u.peer_asn !in 64512..65535 filter u.origin_as !in 64512..65535 filter u.prefix_len < 96 " +
		"filter u.remote_asn != u.origin_as INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", u.prefix, u.prefix_len)," +
		"prefix: u.prefix, prefix_len: u.prefix_len, origin_as: u.origin_as, nexthop: u.nexthop } " +
		"INTO inet_prefix_v6 OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, inet6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	return nil
}
