//go:build integration
// +build integration

package mysql

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	setupMysqlNameDB     = "test_db"
	setupMysqlUserDB     = "default"
	setupMysqlPasswordDB = ""

	setupHostDB string
	setupPortDB nat.Port
)

func setupMysql(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image: "mysql:8",
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": setupMysqlPasswordDB,
			"MYSQL_DATABASE":      setupMysqlNameDB,
			"MYSQL_USER":          setupMysqlUserDB,
			"MYSQL_PASSWORD":      setupMysqlPasswordDB,
		},
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor:   wait.ForListeningPort("3306/tcp"),
	}

	mysqlContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generic container: %w", err)
	}

	setupHostDB, err = mysqlContainer.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get host: %w", err)
	}

	setupPortDB, err = mysqlContainer.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return nil, fmt.Errorf("failed to get port: %w", err)
	}

	return mysqlContainer, nil
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	cont, err := setupMysql(ctx)
	if err != nil {
		log.Fatalf("failed to setup mysql: %v", err)

		return
	}

	if err = initMysqlDB(ctx); err != nil {
		log.Fatalf("failed to init DB mysql: %v", err)

		return
	}

	exitVal := m.Run()

	cont.Terminate(ctx)

	os.Exit(exitVal)
}

func TestMysql_SQLRepository(t *testing.T) {
	t.Parallel()

}

func initMysqlDB(ctx context.Context) error {
	_ = ctx

	return nil
}
