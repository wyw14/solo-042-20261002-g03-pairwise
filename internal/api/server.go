package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-98/internal/alarm"
	"github.com/wyw14/cry-98/internal/fill"
	"github.com/wyw14/cry-98/internal/monitor"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/telemetry"
	"github.com/wyw14/cry-98/internal/transfer"
	"github.com/wyw14/cry-98/internal/vessel"
)

type Dependencies struct {
	Vessels        *vessel.Registry
	Fills          *fill.Service
	Alarms         *alarm.Service
	Probes         *probe.Registry
	ProbeLifecycle *probe.Lifecycle
	Transfers      *transfer.Service
	Telemetry      *telemetry.Ingestor
	Receiver       *telemetry.Receiver
	Health         *monitor.HealthService
	ProbeMonitor   *monitor.ProbeMonitor
	Suppressions   *alarm.SuppressionService
	Now            func() time.Time
}

type Server struct {
	deps   Dependencies
	router chi.Router
}

func NewServer(deps Dependencies) *Server {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	server := &Server{deps: deps}
	server.router = server.routes()
	return server
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) routes() chi.Router {
	router := chi.NewRouter()
	router.Use(requestID)
	router.Use(recoverPanic)
	router.Get("/healthz", s.health)
	router.Route("/api", func(api chi.Router) {
		api.Get("/vessels", s.listVessels)
		api.Get("/vessels/stream", s.streamVessels)
		api.Get("/fill-sessions", s.listFillSessions)
		api.Post("/fill-sessions", s.startFill)
		api.Post("/fill-sessions/{id}/soak", s.beginSoak)
		api.Post("/fill-sessions/{id}/complete", s.completeFill)
		api.Post("/fill-sessions/{id}/abort", s.abortFill)
		api.Post("/fill-sessions/{id}/recover-isolation", s.recoverFillIsolation)
		api.Get("/fill-demands", s.listFillDemands)
		api.Post("/fill-demands/{id}/cancel", s.cancelFillDemand)
		api.Get("/alarms", s.listAlarms)
		api.Post("/alarms", s.raiseAlarm)
		api.Post("/alarms/{id}/acknowledge", s.acknowledgeAlarm)
		api.Post("/alarms/{id}/clear", s.clearAlarm)
		api.Post("/alarms/{id}/suppress", s.suppressAlarm)
		api.Get("/probes", s.listProbes)
		api.Post("/probes/{id}/samples", s.publishProbeSample)
		api.Post("/probes/{id}/rebind", s.rebindProbe)
	})
	return router
}
