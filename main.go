package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
	"github.com/pierrre/geohash"
)

var (
	CreatedDriverIds             []string      = []string{}
	TotalOperationCount          int           = 1_000_000
	InsertOperationsPercent      int           = 100
	ResultedInsertOperationCount int           = 0
	ResultedUpdateOperationCount int           = 0
	ConcurrentWorkerCount        int           = 5
	BatchSize                    int           = 200
	Mu                           *sync.RWMutex = &sync.RWMutex{}
)

func main() {
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = "cassandra_test"
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	fmt.Println("Successfully connected to cassandra")

	startTime := time.Now()
	drivers := []Driver{}
	for {
		uuid, isCreate := WriteOrInsert()
		if !isCreate {
			if len(drivers) > 0 {
				err := CreateBatchDrivers(session, drivers)
				if err != nil {
					fmt.Println("error while creating drivers", err)
				} else {
					fmt.Printf("Created %d drivers successfully\n", len(drivers))
				}
			}
			break
		}

		driver := GenerateFakeDriver(uuid)
		drivers = append(drivers, driver)
		if len(drivers) >= BatchSize {
			err := CreateBatchDrivers(session, drivers)
			if err != nil {
				fmt.Println("error while creating drivers", err)
			} else {
				fmt.Printf("Created %d drivers successfully\n", len(drivers))
			}
			drivers = drivers[:0]
		}
	}
	fmt.Println("|-----------------------------------|")
	fmt.Printf("Total time spend %s\n", time.Since(startTime))
	fmt.Printf("Total drivers inserted %d\n", ResultedInsertOperationCount)
	fmt.Println("|-----------------------------------|")

}

func CreateBatchDrivers(session *gocql.Session, drivers []Driver) error {
	batch := session.Batch(gocql.LoggedBatch)
	ctx := context.Background()
	for _, driver := range drivers {
		batch.Query(`INSERT INTO driver 
		(
			id, lat, lng, geo_hash, score, 
			phone_charge_percent, active, 
			last_updated_time
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			driver.Id, driver.Location.Lat, driver.Location.Long, driver.GeoHash, driver.Score,
			driver.Charge, driver.Active, driver.LastUpdatedTime,
		)
	}

	return batch.ExecContext(ctx)
}

type Driver struct {
	Id              string
	GeoHash         string
	Location        Location
	Score           int64
	Charge          int64
	Active          bool
	LastUpdatedTime string
}

type Location struct {
	Lat  float64
	Long float64
}

func GenerateFakeDriver(id string) Driver {
	// Generate random location within a reasonable range (e.g., around a city center)
	// Using Tashkent, Uzbekistan as a reference point
	lat, lng, geoHash := GetRandomLatLong()
	location := Location{
		Lat:  lat,
		Long: lng,
	}

	// Generate random score (0-100)
	score := rand.Int63n(101)

	// Generate random phone charge percentage (0-100)
	charge := rand.Int63n(101)

	// Randomly set active status (80% chance of being active)
	active := rand.Float64() < 0.8

	// Generate last updated time (within last 24 hours)
	lastUpdated := time.Now().Add(-time.Duration(rand.Intn(24)) * time.Hour)
	lastUpdatedTime := strconv.FormatInt(lastUpdated.Unix(), 10)

	return Driver{
		Id:              id,
		GeoHash:         geoHash,
		Location:        location,
		Score:           score,
		Charge:          charge,
		Active:          active,
		LastUpdatedTime: lastUpdatedTime,
	}
}

func GetRandomLatLong() (float64, float64, string) {
	baseLat := 41.2995
	baseLng := 69.2401

	// Add random offset within ~2000km radius
	latOffset := (rand.Float64() - 0.5) * 20.0 // ~1000km in each direction
	lngOffset := (rand.Float64() - 0.5) * 20.0
	lat := baseLat + latOffset
	lng := baseLng + lngOffset

	geoHash := geohash.Encode(lat, lng, 10)

	return lat, lng, geoHash
}

func GetNextDriverID(isCreate bool) string {
	if isCreate {
		id := uuid.NewString()
		CreatedDriverIds = append(CreatedDriverIds, id)
		return id
	}

	return CreatedDriverIds[ResultedUpdateOperationCount%len(CreatedDriverIds)]
}

// If true write otherwise update
func WriteOrInsert() (string, bool) {
	Mu.Lock()
	defer Mu.Unlock()
	if ResultedInsertOperationCount >= (TotalOperationCount/100)*InsertOperationsPercent {
		driverId := GetNextDriverID(false)
		ResultedUpdateOperationCount++
		return driverId, false
	}

	ResultedInsertOperationCount++
	return GetNextDriverID(true), true
}
