run:
	go run .

test:
	go test ./...

coverage:
	go test -cover ./...

coverage_profile:
	go test ./... -coverprofile=coverage.out && go tool cover -html=coverage.out  