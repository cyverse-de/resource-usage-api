package main

import (
	"net/http"

	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
)

// App encapsulates the application logic.
type App struct {
	db     *sqlx.DB
	router *echo.Echo
}

// GreetingHandler handles requests that simply need to know if the service is running.
func (a *App) GreetingHandler(context echo.Context) error {
	return context.String(http.StatusOK, "Hello from resource-usage-api.")
}

func NewApp(db *sqlx.DB) *App {
	app := &App{
		db:     db,
		router: echo.New(),
	}

	app.router.HTTPErrorHandler = logging.HTTPErrorHandler
	app.router.GET("/", app.GreetingHandler).Name = "greeting"

	return app
}
