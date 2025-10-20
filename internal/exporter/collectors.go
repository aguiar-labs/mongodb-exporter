package exporter

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func (e *Exporter) runIndexStatsCollector(ctx context.Context) {
	connectCtx, cancel := context.WithTimeout(ctx, e.cfg.ConnectTimeout)
	client, err := mongo.Connect(connectCtx, e.clientOptions())
	cancel()
	if err != nil {
		log.Printf("index-stats: connect failed: %v", err)
		return
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	ticker := time.NewTicker(e.cfg.CheckInterval)
	defer ticker.Stop()

	e.collectIndexStats(client)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.collectIndexStats(client)
		}
	}
}

func (e *Exporter) collectIndexStats(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbs, err := e.listDatabases(ctx, client)
	if err != nil {
		log.Printf("index-stats: list DBs failed: %v", err)
		return
	}

	for _, dbname := range dbs {
		db := client.Database(dbname)

		cur, err := db.ListCollections(ctx, bson.D{})
		if err != nil {
			log.Printf("index-stats: list collections on %s failed: %v", dbname, err)
			continue
		}
		var cols []struct {
			Name string `bson:"name"`
		}
		if err := cur.All(ctx, &cols); err != nil {
			log.Printf("index-stats: read collections on %s failed: %v", dbname, err)
			continue
		}

		processed := 0
		for _, c := range cols {
			if e.cfg.MaxCollectionsPerCycle > 0 && processed >= e.cfg.MaxCollectionsPerCycle {
				break
			}
			if !e.shouldCollectCollection(c.Name) {
				continue
			}
			processed++

			coll := db.Collection(c.Name)

			idxCtx, cancelIdx := context.WithTimeout(context.Background(), 20*time.Second)
			idxCur, err := coll.Aggregate(idxCtx, mongo.Pipeline{bson.D{{Key: "$indexStats", Value: bson.D{}}}})
			cancelIdx()
			if err != nil {
				log.Printf("index-stats: $indexStats %s.%s failed: %v", dbname, c.Name, err)
			} else {
				var rows []struct {
					Name     string `bson:"name"`
					Accesses struct {
						Ops   int64     `bson:"ops"`
						Since time.Time `bson:"since"`
					} `bson:"accesses"`
				}
				if err := idxCur.All(context.Background(), &rows); err != nil {
					log.Printf("index-stats: read $indexStats %s.%s failed: %v", dbname, c.Name, err)
				} else {
					for _, r := range rows {
						key := fmt.Sprintf("%s.%s.%s", dbname, c.Name, r.Name)

						e.indexAccess.WithLabelValues(dbname, c.Name, r.Name).Add(0)

						prevOps, hasPrev := e.lastIndexOps[key]
						prevSince, hasSince := e.lastIndexSince[key]
						if hasSince && r.Accesses.Since.Before(prevSince) {
							hasPrev = false
						}

						ops := r.Accesses.Ops
						if hasPrev {
							delta := ops - prevOps
							if delta < 0 {
								delta = ops
							}
							if delta > 0 {
								e.indexAccess.WithLabelValues(dbname, c.Name, r.Name).Add(float64(delta))
								e.indexAccessRate.WithLabelValues(dbname, c.Name, r.Name).Set(float64(delta) / e.cfg.CheckInterval.Seconds())
							} else {
								e.indexAccessRate.WithLabelValues(dbname, c.Name, r.Name).Set(0)
							}
						} else {
							e.indexAccessRate.WithLabelValues(dbname, c.Name, r.Name).Set(0)
						}

						e.indexSince.WithLabelValues(dbname, c.Name, r.Name).Set(float64(r.Accesses.Since.Unix()))
						e.lastIndexOps[key] = ops
						e.lastIndexSince[key] = r.Accesses.Since
					}
				}
			}

			if e.cfg.CollectCollStats {
				csCtx, cancelCS := context.WithTimeout(context.Background(), 30*time.Second)
				var collStatsRes struct {
					IndexSizes map[string]int64 `bson:"indexSizes"`
				}
				err := db.RunCommand(csCtx, bson.D{{Key: "collStats", Value: c.Name}}).Decode(&collStatsRes)
				cancelCS()
				if err != nil {
					log.Printf("index-stats: collStats %s.%s failed: %v", dbname, c.Name, err)
				} else {
					for idx, sz := range collStatsRes.IndexSizes {
						e.indexSize.WithLabelValues(dbname, c.Name, idx).Set(float64(sz))
					}
				}
			}
		}
	}
}

func (e *Exporter) runSlowCollector(ctx context.Context) {
	connectCtx, cancel := context.WithTimeout(ctx, e.cfg.ConnectTimeout)
	client, err := mongo.Connect(connectCtx, e.clientOptions())
	cancel()
	if err != nil {
		log.Printf("slow-collector: connect failed: %v", err)
		return
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	if e.cfg.EnableProfiler {
		e.enableProfiler(client)
	}

	ticker := time.NewTicker(e.cfg.CheckInterval)
	defer ticker.Stop()

	e.collectSlowMetrics(client)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.collectSlowMetrics(client)
		}
	}
}

func (e *Exporter) enableProfiler(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dbs, err := e.listDatabases(ctx, client)
	if err != nil {
		log.Printf("slow-collector: list DBs failed: %v", err)
		return
	}

	for _, dbname := range dbs {
		if dbname == "config" || dbname == "local" {
			continue
		}
		cmd := bson.D{{Key: "profile", Value: 1}, {Key: "slowms", Value: e.cfg.SlowMS}}
		if err := client.Database(dbname).RunCommand(ctx, cmd).Err(); err != nil {
			log.Printf("slow-collector: enable profiler on %s: %v", dbname, err)
		}
	}
}

func (e *Exporter) collectSlowMetrics(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dbs, err := e.listDatabases(ctx, client)
	if err != nil {
		log.Printf("slow-collector: list DBs failed: %v", err)
		return
	}

	e.mongoSlowGauge.Reset()

	for _, dbname := range dbs {
		if dbname == "config" || dbname == "local" {
			continue
		}

		profile := client.Database(dbname).Collection("system.profile")

		match := bson.D{{Key: "millis", Value: bson.D{{Key: "$gte", Value: e.cfg.SlowMS}}}}
		if last, ok := e.lastSeenTs[dbname]; ok && !last.IsZero() {
			match = append(match, bson.E{Key: "ts", Value: bson.D{{Key: "$gt", Value: last}}})
		}

		pipeline := mongo.Pipeline{
			bson.D{{Key: "$match", Value: match}},
			bson.D{{Key: "$project", Value: bson.D{
				{"ns", 1},
				{"op", 1},
				{"millis", 1},
				{"ts", 1},
				{"appName", 1},
				{"client", 1},
				{"user", 1},
				{"planSummary", 1},
				{"docsExamined", 1},
				{"keysExamined", 1},
				{"commandName", bson.D{
					{"$first", bson.D{
						{"$map", bson.D{
							{"input", bson.D{{"$objectToArray", bson.D{{"$ifNull", bson.A{"$command", bson.D{}}}}}}},
							{"as", "kv"},
							{"in", "$$kv.k"},
						}},
					}},
				}},
				{"command", bson.D{
					{"$function", bson.D{
						{"body", "function(cmd) { try { if (!cmd || typeof cmd !== 'object') return ''; function sanitizeValue(v){ if (v === null || v === undefined) return '?'; var t = typeof v; if (t === 'string' || t === 'number' || t === 'boolean') return '?'; if (Array.isArray(v)) return v.map(sanitizeValue); if (t === 'object') { var ks = Object.keys(v); if (ks.length === 1 && (ks[0] === '$oid' || ks[0] === '$date' || ks[0] === '$numberLong' || ks[0] === '$binary' || ks[0] === '$uuid')) return '?'; var o = {}; for (var k in v) { o[k] = sanitizeValue(v[k]); } return o; } return '?'; } function sanitizeFilter(f){ if (!f || typeof f !== 'object') return {}; var out = {}; for (var k in f) { out[k] = sanitizeValue(f[k]); } return out; } var c = {}; for (var k in cmd) { c[k] = cmd[k]; } delete c.lsid; delete c.$clusterTime; delete c.$db; delete c.$readPreference; delete c.$client; delete c.txnNumber; delete c.comment; delete c.allowDiskUse; var out = c; if (c.find) { out = { find: c.find, filter: sanitizeFilter(c.filter || {}) }; } else if (c.aggregate) { var matches = []; (c.pipeline || []).forEach(function(stage){ if (stage.$match) matches.push(sanitizeFilter(stage.$match)); }); out = { aggregate: c.aggregate, match: matches }; } else if (c.update) { out = { update: c.update, updates: (c.updates || []).map(function(u){ return { q: sanitizeFilter(u.q || {}), u: '?' }; }) }; } else if (c.delete) { out = { delete: c.delete, deletes: (c.deletes || []).map(function(d){ return { q: sanitizeFilter(d.q || {}) }; }) }; } return JSON.stringify(out).slice(0, 500); } catch (e) { return ''; } }"},
						{"args", bson.A{"$command"}},
						{"lang", "js"},
					}},
				}},
			}}},
			bson.D{{Key: "$group", Value: bson.D{
				{"_id", bson.D{
					{"db", bson.D{{"$arrayElemAt", bson.A{bson.D{{"$split", bson.A{"$ns", "."}}}, 0}}}},
					{"collection", bson.D{{"$arrayElemAt", bson.A{bson.D{{"$split", bson.A{"$ns", "."}}}, 1}}}},
					{"commandName", "$commandName"},
					{"op", "$op"},
					{"planSummary", "$planSummary"},
					{"appName", "$appName"},
					{"command", "$command"},
				}},
				{"count", bson.D{{"$sum", 1}}},
				{"avgMillis", bson.D{{"$avg", "$millis"}}},
				{"maxMillis", bson.D{{"$max", "$millis"}}},
			}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "count", Value: -1}}}},
			bson.D{{Key: "$limit", Value: 100}},
		}

		cur, err := profile.Aggregate(ctx, pipeline)
		if err != nil {
			log.Printf("slow-collector: aggregate on %s.system.profile failed: %v", dbname, err)
			continue
		}

		var results []struct {
			ID struct {
				DB          string `bson:"db"`
				Collection  string `bson:"collection"`
				CommandName string `bson:"commandName"`
				Op          string `bson:"op"`
				PlanSummary string `bson:"planSummary"`
				AppName     string `bson:"appName"`
				Command     string `bson:"command"`
			} `bson:"_id"`
			Count     int64   `bson:"count"`
			AvgMillis float64 `bson:"avgMillis"`
			MaxMillis int64   `bson:"maxMillis"`
		}

		if err := cur.All(ctx, &results); err != nil {
			log.Printf("slow-collector: read cursor on %s: %v", dbname, err)
			continue
		}

		for _, r := range results {
			e.mongoSlowGauge.WithLabelValues(
				r.ID.DB,
				r.ID.Collection,
				r.ID.CommandName,
				r.ID.AppName,
				r.ID.PlanSummary,
				r.ID.Op,
				r.ID.Command,
			).Set(float64(r.Count))

			e.mongoSlowTotal.WithLabelValues(
				r.ID.DB,
				r.ID.Collection,
				r.ID.CommandName,
				r.ID.AppName,
				r.ID.PlanSummary,
				r.ID.Op,
				r.ID.Command,
			).Add(float64(r.Count))

			e.mongoSlowAvgMillis.WithLabelValues(
				r.ID.DB,
				r.ID.Collection,
				r.ID.CommandName,
				r.ID.AppName,
				r.ID.PlanSummary,
			).Set(r.AvgMillis)

			e.mongoSlowMaxMillis.WithLabelValues(
				r.ID.DB,
				r.ID.Collection,
				r.ID.CommandName,
				r.ID.AppName,
				r.ID.PlanSummary,
			).Set(float64(r.MaxMillis))

			e.mongoSlowExecTimeTotal.WithLabelValues(
				r.ID.DB,
				r.ID.Collection,
				r.ID.CommandName,
				r.ID.AppName,
				r.ID.PlanSummary,
			).Add(r.AvgMillis * float64(r.Count))

		}

		e.lastSeenTs[dbname] = time.Now()
	}
}

func (e *Exporter) runPinger(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.CheckInterval)
	defer ticker.Stop()

	e.checkMongo()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.checkMongo()
		}
	}
}

func (e *Exporter) checkMongo() {
	pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := e.pingMongo(pingCtx); err != nil {
		e.mongoUp.Set(0)
		log.Printf("mongo ping FAILED: %v", err)
	} else {
		e.mongoUp.Set(1)
	}
}

func (e *Exporter) pingMongo(ctx context.Context) error {
	client, err := mongo.Connect(ctx, e.clientOptions())
	if err != nil {
		return err
	}
	defer func() { _ = client.Disconnect(context.Background()) }()
	return client.Ping(ctx, nil)
}

func (e *Exporter) listDatabases(ctx context.Context, client *mongo.Client) ([]string, error) {
	if e.cfg.TargetDB != "" {
		return []string{e.cfg.TargetDB}, nil
	}
	return client.ListDatabaseNames(ctx, bson.D{})
}
