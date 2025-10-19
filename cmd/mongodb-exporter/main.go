package main

import (
	"context"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/aguiar-labs/mongodb-exporter/internal/config"
	"github.com/aguiar-labs/mongodb-exporter/internal/exporter"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	exp := exporter.New(cfg)
	exp.Start(context.Background())

	handler := promhttp.HandlerFor(exp.Registry(), promhttp.HandlerOpts{})

	log.Printf("listening on %s, scraping /metrics", cfg.ListenAddr)
	log.Fatal(http.ListenAndServe(cfg.ListenAddr, handler))
}
