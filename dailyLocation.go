package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//var client *mongo.Client

type BatDataLocation struct {
	ID        string  `bson:"_id,omitempty"`
	LOND      int32   `bson:"lond"`
	SATELLITE int32   `bson:"satellite"`
	TIME      int64   `bson:"time"`
	RPWR      uint32  `bson:"rpwr"`
	LAT       float64 `bson:"lat"`
	LATD      int32   `bson:"latd"`
	LON       float64 `bson:"lon"`
	ALTITUDE  int32   `bson:"altitude"`
	SPEED     int32   `bson:"speed"`
	TIMESTAMP int64   `bson:"timestamp"`
	MID       string  `bson:"mid"`
}

type BatDataAll struct {
	BID      string `bson:"bid"`
	LASTTIME uint64 `bson:"lastProcessedLocation, omitempty"`
}

type ProcessedData struct {
	TO        uint64
	FROM      uint64
	MID       string
	BID       string
	LOND      []int32
	SATELLITE []int32
	TIME      []int64
	RPWR      []uint32
	LAT       []float64
	LATD      []int32
	LON       []float64
	ALTITUDE  []int32
	TIMESTAMP []int64
}

func handlePostRequest(client *mongo.Client) {

	var batDataAllObjArray []BatDataAll

	colBatDataAll := client.Database("portal").Collection("batDataAll")
	ctx4, cancel4 := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel4()

	findOptions := options.Find()
	// Sort by `price` field descending
	findOptions.SetSort(bson.D{{"_id", -1}})

	cur0, err0 := colBatDataAll.Find(ctx4, bson.M{}, findOptions)
	if err0 != nil {
		log.Fatal(err0)
	}
	defer cur0.Close(context.TODO())

	//fmt.Println("run")
	for cur0.Next(context.TODO()) {
		var result BatDataAll
		err := cur0.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		// fmt.Println(result.BID)
		// break;
		batDataAllObjArray = append(batDataAllObjArray, result)
	}

	if err := cur0.Err(); err != nil {
		log.Fatal(err)
	}

	// -----------------------------------------------

	colBatDataLocation := client.Database("portal").Collection("batDataLocation")
	//initialTime := uint64(1690196118000)

	initialTime := uint64(1693312100000)

	currentTime := time.Now()
	pastFiveDays := currentTime.Add(-4 * 24 * time.Hour)
	endTime := time.Date(pastFiveDays.Year(), pastFiveDays.Month(), pastFiveDays.Day(), 0, 0, 0, 0, time.UTC)

	processTillTime := uint64(endTime.UnixNano() / int64(time.Millisecond))

	// index := 0
	for _, v := range batDataAllObjArray {
		// if v.BID != "1702A1C6" {
		// 	continue
		// }

		globalTimeStart := initialTime
		if v.LASTTIME != 0 {
			globalTimeStart = v.LASTTIME
		}
		for globalTimeStart < processTillTime {

			tempFrom := globalTimeStart

			tempTo := globalTimeStart + 86400000
			globalTimeStart = globalTimeStart + 86400000

			filter1 := bson.M{"timestamp": bson.M{"$gte": tempFrom, "$lt": tempTo}, "bid": v.BID}
			ctx1, cancel1 := context.WithCancel(context.Background())
			findOptionsMain := options.Find()
			findOptionsMain.SetSort(bson.D{{"timestamp", 1}})
			cur, err := colBatDataLocation.Find(ctx1, filter1, findOptionsMain)
			if err != nil {
				log.Println(err)
			}

			var dataToIns ProcessedData

			dataToIns.BID = v.BID
			dataToIns.FROM = tempFrom
			dataToIns.TO = tempTo

			for cur.Next(context.TODO()) {

				var result BatDataLocation

				err := cur.Decode(&result)
				if err != nil {
					log.Fatal(err)
				}

				dataToIns.LOND = append(dataToIns.LOND, result.LOND)
				dataToIns.SATELLITE = append(dataToIns.SATELLITE, result.SATELLITE)
				dataToIns.TIME = append(dataToIns.TIME, result.TIME)
				dataToIns.RPWR = append(dataToIns.RPWR, result.RPWR)
				dataToIns.LAT = append(dataToIns.LAT, result.LAT)
				dataToIns.LATD = append(dataToIns.LATD, result.LATD)
				dataToIns.LON = append(dataToIns.LON, result.LON)
				dataToIns.ALTITUDE = append(dataToIns.ALTITUDE, result.ALTITUDE)
				dataToIns.TIMESTAMP = append(dataToIns.TIMESTAMP, result.TIMESTAMP)
				dataToIns.MID = result.MID
			}

			if err := cur.Err(); err != nil {
				log.Fatal(err)
			}

			cancel1()

			if len(dataToIns.TIMESTAMP) > 0 {

				timeObj := time.Unix(int64(dataToIns.FROM/1000), 0)
				year := timeObj.Year()
				colProcessedData := client.Database("portal").Collection("processedLocation" + strconv.Itoa(year))

				_, err := colProcessedData.InsertOne(context.TODO(), dataToIns)
				if err != nil {
					log.Fatal(err)
				}
				// currentTime := time.Now()
				// fmt.Println(currentTime.Format("2006-01-02 15:04:05"), " => ", " Inserted ==>:", insertResult.InsertedID)
				// fmt.Print("$")

			} else {
				// currentTime := time.Now()
				// fmt.Println(currentTime.Format("2006-01-02 15:04:05"), " => ", )
				// fmt.Print(".")
			}

			//--------------------------------Saving timestamp for upgraded-------------------
			filterBatData := bson.M{"bid": v.BID}
			toUpdate := bson.M{
				"$set": bson.M{
					"lastProcessedLocation": tempTo,
				},
			}
			lastProcessRes, lastProcessErr := colBatDataAll.UpdateOne(context.TODO(), filterBatData, toUpdate)
			if lastProcessRes.MatchedCount == 0 || lastProcessErr != nil {

				fmt.Println("Error occured while updating...")
			}

			//-------------------------------------------------------------------------------
		}
		// fmt.Println(v.BID)
		// -----------------  Delete old data of BID whose data is proccessed -------------
		// index++
		// fmt.Println("Written data: ", v.BID, index, processTillTime)

		filterDelete := bson.M{
			"bid":       v.BID,
			"timestamp": bson.M{"$lt": processTillTime},
		}
		ctxDelete, cancelDelete := context.WithCancel(context.Background())

		result, err := colBatDataLocation.DeleteMany(ctxDelete, filterDelete)

		if err != nil {
			log.Println(err)
		}
		cancelDelete()
		fmt.Printf("Deleted %v documents of: %s\n", result.DeletedCount, v.BID)

		// -----------------------------------------------------------------------------------

	}

	err := client.Disconnect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	currentTimeEnd := time.Now()
	fmt.Println("Connection to MongoDB closed.", currentTimeEnd)

}

func main() {

	currentTime := time.Now()
	fmt.Println("Location Script : ", currentTime)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	clientOptions := options.Client().ApplyURI("mongodb://administrator:%26%5E%23%25%21%2612dgf_%23%26@15.207.150.151:49125/?authSource=portal&readPreference=primary&directConnection=true&ssl=false")
	client, err := mongo.Connect(ctx, clientOptions)

	if err != nil {
		fmt.Println(err)
		return
	}

	handlePostRequest(client)

	client.Disconnect(context.TODO())
}

func insertColumn(matrix [][]int, newColumn []int, index int) [][]int {
	for i, row := range matrix {
		row = append(row, 0)
		copy(row[index+1:], row[index:])
		row[index] = newColumn[i]
		matrix[i] = row
	}
	return matrix

}
