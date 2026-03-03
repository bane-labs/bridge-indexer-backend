.PHONY: swagger
swagger:
	swag init -g cmd/server/main.go -o docs/swagger --parseInternal --parseDependency
