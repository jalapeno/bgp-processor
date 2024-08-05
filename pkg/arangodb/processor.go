package arangodb

import (
	"context"
	"strconv"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) processBgpNode(ctx context.Context, key, id string, e message.PeerStateChange) error {
	if e.RemoteASN == e.LocalASN {
		glog.Infof("ibgp peer: %+v", e.Key)
		obj := ibgpPeer{
			Key:             e.RemoteBGPID + "_" + strconv.Itoa(int(e.RemoteASN)),
			BGPRouterID:     e.RemoteBGPID,
			ASN:             int32(e.RemoteASN),
			AdvCapabilities: e.AdvCapabilities,
		}

		if _, err := a.ibgpPeer.CreateDocument(ctx, &obj); err != nil {
			glog.Infof("create iBGP peer: %+v", e.Key)
			if !driver.IsConflict(err) {
				return err
			}
		}
	} else {

		obj := ebgpPeer{
			Key:             e.RemoteBGPID + "_" + strconv.Itoa(int(e.RemoteASN)),
			BGPRouterID:     e.RemoteBGPID,
			ASN:             int32(e.RemoteASN),
			AdvCapabilities: e.AdvCapabilities,
		}

		if _, err := a.ebgpPeer.CreateDocument(ctx, &obj); err != nil {
			glog.Infof("create eBGP peer: %+v", e.RemoteBGPID+"_"+strconv.Itoa(int(e.RemoteASN)))
			if !driver.IsConflict(err) {
				return err
			}
		}
	}
	return nil
}

// process Removal removes records from the inetprefixV4 collection
func (a *arangoDB) processPeerSessionRemoval(ctx context.Context, key string, e *message.PeerStateChange) error {

	split := strings.Split(key, "_")
	rtrid := split[0]
	glog.Infof(rtrid)

	glog.Infof("removing v6 peer %+v, and full msg: %+v", key, rtrid)
	query := "for p in peer filter p.remote_bgp_id == " + "\"" + rtrid + "\"" +
		" COLLECT WITH COUNT INTO length "
	query += " return length"

	glog.Infof("query: %+v", query)
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()

	for {
		var nm peerCount
		m, err := ncursor.ReadDocument(ctx, &nm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		glog.Infof("peer session count: %+v, m: %+v", nm, m)

		if nm == 0 {

			glog.Infof("last bgp session, removing bgp_node %+v", key)
			query := "for d in ebgp_peer" +
				" filter d.bgp_router_id == " + "\"" + rtrid + "\""
			query += " remove d in bgp_node"
			glog.Infof("query: %+v", query)
			ncursor, err := a.db.Query(ctx, query, nil)
			if err != nil {
				return err
			}
			defer ncursor.Close()

			for {
				var nm message.PeerStateChange
				m, err := ncursor.ReadDocument(ctx, &nm)
				if err != nil {
					if !driver.IsNoMoreDocuments(err) {
						return err
					}
					break
				}
				//if _, err := a.ebgpSessionV6.RemoveDocument(ctx, m.ID.Key()); err != nil {
				if _, err := a.ebgpPeer.RemoveDocument(ctx, key); err != nil {
					glog.Infof("remove v6 ebgp session: %+v, doc: %+v, session: %+v", key, m, a.ebgpPeer)
					if !driver.IsNotFound(err) {
						return err
					}
				}
			}
		} else {
			glog.Infof("still existing BGP sessions, do not remove bgp_node")
		}
	}
	return nil
}

func (a *arangoDB) processV4Prefix(ctx context.Context, key, id string, e message.UnicastPrefix) error {
	// get internal ASN so we can determine whether this is an external prefix or not
	getasn := "for l in igp_domain return l.asn"
	cursor, err := a.db.Query(ctx, getasn, nil)
	if err != nil {
		return err
	}
	var ln InternalASNs
	lm, err := cursor.ReadDocument(ctx, &ln)
	glog.V(5).Infof("meta %+v", lm)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}
	var result bool = false
	for _, x := range ln.asnlist {
		if e.OriginAS == (int32(x)) {
			result = true
			break
		}
	}
	if result {
		glog.Infof("internal ASN %+v found in unicast prefix message, process as non-Inet prefix", e.Prefix)

		obj := inetPrefix{
			//Key: inetKey,
			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
			Prefix:    e.Prefix,
			PrefixLen: e.PrefixLen,
			OriginAS:  e.OriginAS,
			NextHop:   e.Nexthop,
		}
		if _, err := a.ebgpprefixV4.CreateDocument(ctx, &obj); err != nil {
			glog.Infof("adding non-Inet prefix: %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))
			if !driver.IsConflict(err) {
				return nil
			}
		}

	} else {
		obj := inetPrefix{
			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
			Prefix:    e.Prefix,
			PrefixLen: e.PrefixLen,
			OriginAS:  e.OriginAS,
			NextHop:   e.Nexthop,
		}
		if _, err := a.inetprefixV4.CreateDocument(ctx, &obj); err != nil {
			glog.Infof("adding Inet prefix: %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))
			if !driver.IsConflict(err) {
				return nil
			}
		}
	}

	return nil
}

func (a *arangoDB) processV6Prefix(ctx context.Context, key, id string, e message.UnicastPrefix) error {
	// get internal ASN so we can determine whether this is an external prefix or not
	getasn := "for l in igp_domain return l.asn"
	cursor, err := a.db.Query(ctx, getasn, nil)
	if err != nil {
		return err
	}
	var ln InternalASNs
	lm, err := cursor.ReadDocument(ctx, &ln)
	glog.V(5).Infof("meta %+v", lm)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}
	var result bool = false
	for _, x := range ln.asnlist {
		if e.OriginAS == (int32(x)) {
			result = true
			break
		}
	}
	if result {
		glog.Infof("internal ASN %+v found in unicast prefix message, process as non-Inet prefix", e.Prefix)

		obj := inetPrefix{
			//Key: inetKey,
			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
			Prefix:    e.Prefix,
			PrefixLen: e.PrefixLen,
			OriginAS:  e.OriginAS,
			NextHop:   e.Nexthop,
		}
		if _, err := a.ebgpprefixV6.CreateDocument(ctx, &obj); err != nil {
			glog.Infof("adding non-Inet prefix: %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))
			if !driver.IsConflict(err) {
				return nil
			}
		}

	} else {
		obj := inetPrefix{
			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
			Prefix:    e.Prefix,
			PrefixLen: e.PrefixLen,
			OriginAS:  e.OriginAS,
			NextHop:   e.Nexthop,
		}
		if _, err := a.inetprefixV6.CreateDocument(ctx, &obj); err != nil {
			glog.Infof("adding Inet prefix: %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))
			if !driver.IsConflict(err) {
				return nil
			}
		}
	}
	return nil
}

// process Removal removes records from the inetprefixV4 collection
func (a *arangoDB) processV4Removal(ctx context.Context, key string, e *message.UnicastPrefix) error {
	query := "for d in " + a.inetprefixV4.Name() +
		" filter d.prefix == " + "\"" + e.Prefix + "\""
	query += " return d"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()

	for {
		var nm inetPrefix
		m, err := ncursor.ReadDocument(ctx, &nm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		if _, err := a.inetprefixV4.RemoveDocument(ctx, m.ID.Key()); err != nil {
			if !driver.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

// process Removal removes records from the inetprefixV4 collection
func (a *arangoDB) processV6Removal(ctx context.Context, key string, e *message.UnicastPrefix) error {
	query := "for d in " + a.inetprefixV6.Name() +
		" filter d._key == " + "\"" + key + "\""
	query += " return d"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()

	for {
		var nm inetPrefix
		m, err := ncursor.ReadDocument(ctx, &nm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		if _, err := a.inetprefixV6.RemoveDocument(ctx, m.ID.Key()); err != nil {
			if !driver.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}
