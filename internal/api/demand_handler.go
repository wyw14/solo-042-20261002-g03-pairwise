package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/fill"
)

type cancelDemandRequest struct {
	Reason string `json:"reason"`
}

func (s *Server) listFillDemands(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.deps.Fills.Demands().List()})
}

func (s *Server) cancelFillDemand(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid demand id")
		return
	}
	var input cancelDemandRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	demand, err := s.deps.Fills.Demands().Cancel(id, input.Reason)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, fill.ErrDemandNotFound) {
			status = http.StatusNotFound
		}
		writeError(writer, status, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, demand)
}
