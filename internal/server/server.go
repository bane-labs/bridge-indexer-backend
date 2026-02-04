package server

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/labstack/echo/v4"

	"github.com/bane-labs/bridge-indexer-backend/internal/store"
)

// Server represents the HTTP server
type Server struct {
	echo      *echo.Echo
	dataStore *store.DataStore
	logger    *slog.Logger
}

// New creates a new server instance
func New() (*Server, error) {
	// Initialize logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Configure database path
	dbPath := "./data/badger"

	// Create directory if it doesn't exist
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		logger.Error("Failed to create database directory", "error", err)
		return nil, err
	}

	// Initialize BadgerDB store using the pattern
	badgerDb, err := store.NewBadgerDbStore(store.NewDbOptions(&dbPath), logger)
	if err != nil {
		logger.Error("Failed to create new BadgerDB store", "error", err)
		return nil, err
	}

	dataStore := store.NewDataStore(badgerDb)

	// Initialize Echo
	e := echo.New()

	s := &Server{
		echo:      e,
		dataStore: dataStore,
		logger:    logger,
	}

	s.setupRoutes()
	return s, nil
}

// ServeHTTP implements the http.Handler interface
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.echo.ServeHTTP(w, r)
}

// setupRoutes configures the API routes
func (s *Server) setupRoutes() {
	// TODO: Add routes here
}

// GetDataStore returns the configured DataStore for use by other components
func (s *Server) GetDataStore() *store.DataStore {
	return s.dataStore
}

// GetLogger returns the configured logger
func (s *Server) GetLogger() *slog.Logger {
	return s.logger
}

// GetEcho returns the Echo instance for direct route configuration
func (s *Server) GetEcho() *echo.Echo {
	return s.echo
}
