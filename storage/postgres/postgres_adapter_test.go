package postgres

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/ory/dockertest"

	"BNR-Blog-Dockertest/storage"
)

var testPort string

const testUser = "postgres"
const testHost = "localhost"
const testDbName = "phone_numbers"

// getAdapter retrieves the Postgres adapter with test credentials
func getAdapter() (*PgAdapter, error) {
	return NewAdapter(testHost, testPort, testUser, testDbName)
}

// initTestAdapter inserts test numbers into the database
func initTestAdapter(adapter *PgAdapter) {
	testNumbers := []storage.PhoneNumber{
		{
			ID:     1,
			Number: "1234565454",
		},
		{
			ID:     2,
			Number: "4565434343",
		},
	}
	err := adapter.insertNumbers(testNumbers) // Insert numbers for testing purposes
	if err != nil {
		log.Fatalf("error inserting test numbers %v", err)
	}
}

// setup instantiates a Postgres docker container and attempts to connect to it via a new adapter
func setup() *dockertest.Resource {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to docker: %s", err)
	}

	// Pulls an image, creates a container based on it and runs it
	resource, err := pool.Run("postgres", "latest", []string{"POSTGRES_HOST_AUTH_METHOD=trust", fmt.Sprintf("POSTGRES_DB=%s", testDbName)})
	if err != nil {
		log.Fatalf("could not start resource: %s", err)
	}
	testPort = resource.GetPort("5432/tcp") // Set port used to communicate with Postgres

	var adapter *PgAdapter
	// Exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		adapter, err = getAdapter()
		return err
	}); err != nil {
		log.Fatalf("could not connect to docker: %s", err)
	}

	initTestAdapter(adapter)

	return resource
}

// cleanup removes the docker container resource
func cleanup(resource *dockertest.Resource) {
	err := resource.Close()
	if err != nil {
		log.Fatalf("error removing container %v", err)
	}
}

func TestMain(m *testing.M) {
	resource := setup() // Setup one container for test suite to limit resources created during tests
	code := m.Run()
	cleanup(resource) // Tear down container when test suite is done running to avoid extraneous resources
	os.Exit(code)
}

func TestCreatePhoneNumber(t *testing.T) {
	testNumber := "1234566656"
	adapter, err := getAdapter()
	if err != nil {
		t.Fatalf("error creating new test adapter: %v", err)
	}

	cases := []struct {
		error       bool
		description string
	}{
		{
			description: "Should succeed with valid creation of a phone number",
		},
		{
			description: "Should fail if database connection closed",
			error:       true,
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			if c.error {
				adapter.conn.Close()
			}
			id, err := adapter.CreatePhoneNumber(testNumber)
			if !c.error && err != nil {
				t.Errorf("expecting no error but received: %v", err)
			} else if !c.error { // Remove test number from db so not captured by following tests
				err = adapter.RemovePhoneNumber(id)
				if err != nil {
					t.Fatalf("error removing test number from database")
				}
			}
		})
	}
}

func TestGetPhoneNumbers(t *testing.T) {
	adapter, err := getAdapter()
	if err != nil {
		t.Fatalf("error creating new test adapter: %v", err)
	}

	cases := []struct {
		error       bool
		description string
	}{
		{
			description: "Should succeed with valid retrieval of phone numbers",
		},
		{
			description: "Should fail if database connection closed",
			error:       true,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("Test Case #:%d", i), func(t *testing.T) {
			if c.error {
				adapter.conn.Close()
			}
			numbers, err := adapter.GetPhoneNumbers()
			if !c.error && err != nil {
				t.Errorf("expecting no error but received: %v", err)
			} else if !c.error {
				cupaloy.SnapshotT(t, numbers)
			}
		})
	}
}

func TestUpdatePhoneNumber(t *testing.T) {
	adapter, err := getAdapter()
	if err != nil {
		t.Fatalf("error creating new test adapter: %v", err)
	}
	numbers, err := adapter.GetPhoneNumbers()
	if err != nil {
		t.Fatalf("error getting phone numbers: %v", err)
	}

	cases := []struct {
		error       bool
		description string
	}{
		{
			description: "Should succeed with valid update of a phone number",
		},
		{
			description: "Should fail if database connection closed",
			error:       true,
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			if c.error {
				adapter.conn.Close()
			}
			err := adapter.UpdatePhoneNumber(numbers[0])
			if !c.error && err != nil {
				t.Fatalf("expecting no error but received: %v", err)
			}
		})
	}
}

func TestRemovePhoneNumber(t *testing.T) {
	adapter, err := getAdapter()
	if err != nil {
		t.Fatalf("error creating new test adapter: %v", err)
	}
	id, err := adapter.CreatePhoneNumber("2154354333")
	if err != nil {
		t.Fatalf("error getting phone numbers: %v", err)
	}

	cases := []struct {
		error       bool
		description string
	}{
		{
			description: "Should succeed with valid deletion of a phone number",
		},
		{
			description: "Should fail if database connection closed",
			error:       true,
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			if c.error {
				adapter.conn.Close()
			}
			err := adapter.RemovePhoneNumber(id)
			if !c.error && err != nil {
				t.Fatalf("expecting no error but received: %v", err)
			}
		})
	}
}
