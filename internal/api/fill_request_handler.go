package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-98/internal/fillrequest"
)

type submitFillRequestInput struct {
	VesselID string  `json:"vessel_id"`
	Urgency  string  `json:"urgency"`
	Liters   float64 `json:"liters"`
}

func (s *Server) listFillRequests(writer http.ResponseWriter, request *http.Request) {
	if s.deps.Requests == nil {
		writeJSON(writer, http.StatusOK, map[string]any{"items": []any{}})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.deps.Requests.List()})
}

func (s *Server) submitFillRequest(writer http.ResponseWriter, request *http.Request) {
	if s.deps.Requests == nil {
		writeError(writer, http.StatusServiceUnavailable, "fill request queue is unavailable")
		return
	}
	var input submitFillRequestInput
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	urgency, ok := parseUrgency(input.Urgency)
	if !ok {
		writeError(writer, http.StatusBadRequest, "invalid urgency")
		return
	}
	demand, err := s.deps.Requests.Submit(input.VesselID, urgency, input.Liters)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, fillrequest.ErrActiveDemand) {
			status = http.StatusConflict
		}
		writeError(writer, status, err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, demand)
}

func (s *Server) cancelFillRequest(writer http.ResponseWriter, request *http.Request) {
	if s.deps.Requests == nil {
		writeError(writer, http.StatusServiceUnavailable, "fill request queue is unavailable")
		return
	}
	vesselID := chi.URLParam(request, "vesselID")
	var input abortFillRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	demand, err := s.deps.Requests.Cancel(vesselID, input.Reason)
	if err != nil {
		writeError(writer, http.StatusConflict, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, demand)
}

func parseUrgency(value string) (fillrequest.Urgency, bool) {
	switch value {
	case "", "normal":
		return fillrequest.UrgencyNormal, true
	case "high":
		return fillrequest.UrgencyHigh, true
	case "critical":
		return fillrequest.UrgencyCritical, true
	default:
		return fillrequest.UrgencyNormal, false
	}
}
