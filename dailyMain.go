package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//var client *mongo.Client

type BatDataMain struct {
	ID      string `bson:"_id,omitempty"`
	SOC     uint32 `bson:"soc"`
	SOH     uint32 `bson:"soh"`
	CURR    int32  `bson:"curr"`
	PV      uint32 `bson:"pv"`
	CYCLES  uint32 `bson:"cycles"`
	BRDTEMP int32  `bson:"brdtemp"`
	RPWR    uint32 `bson:"rpwr"`
	STATUS  uint32 `bson:"status"`
	SOHWS   uint32 `bson:"sohws"`

	CV string `bson:"cellvolt"`
	TS string `bson:"tempsen"`

	CELLVOLT  []uint32 `bson:"cellvoltnew"`
	TEMPSEN   []int32  `bson:"tempsennew"`
	Timestamp int64    `bson:"timestamp"`
}

type BatDataAll struct {
	BID      string `bson:"bid"`
	LASTTIME uint64 `bson:"lastProcessedAnalytics, omitempty"`
}

type ProcessedData struct {
	SOC       []uint32
	BID       string
	FROM      uint64
	TO        uint64
	SOH       []uint32
	CURR      []int32
	PV        []uint32
	CYCLES    []uint32
	BRDTEMP   []int32
	RPWR      []uint32
	STATUS    []uint32
	SOHWS     []uint32
	TIMESTAMP []int64
	TEMPSEN   [][]int32
	CELLVOLT  [][]uint32
	BID_DEC   uint64
}

type CVAStruct struct {
	CellVoltP []uint32 `json:"cellvolt"`
}

type TempStruct struct {
	TempSenP []int32 `json:"tempsen"`
}

type LocalData struct {
	Bid uint32 `json:"value"`
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

	colBatDataMain := client.Database("portal").Collection("batDataMain")
	//initialTime := uint64(1690196118000)

	currentTime := time.Now()

	// fmt.Println(currentTime)
	initialTime := uint64(currentTime.Add(-15*24*time.Hour).UnixNano() / int64(time.Millisecond))
	// fmt.Println(initialTime)

	pastFiveDays := currentTime.Add(-4 * 24 * time.Hour)
	endTime := time.Date(pastFiveDays.Year(), pastFiveDays.Month(), pastFiveDays.Day(), 0, 0, 0, 0, time.UTC)

	processTillTime := uint64(endTime.UnixNano() / int64(time.Millisecond))

	// index := 0
	for _, v := range batDataAllObjArray {

		// if v.BID != "A843A385" {
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
			cur, err := colBatDataMain.Find(ctx1, filter1, findOptionsMain)
			if err != nil {
				log.Println(err)
			}

			var dataToIns ProcessedData

			dataToIns.BID = v.BID
			dataToIns.FROM = tempFrom
			dataToIns.TO = tempTo

			for cur.Next(context.TODO()) {

				var result BatDataMain

				err := cur.Decode(&result)
				if err != nil {
					log.Fatal(err)
				}

				var cellDataProcessed CVAStruct
				var tempDataProcessed TempStruct

				errCV := json.Unmarshal([]byte(result.CV), &cellDataProcessed)
				if errCV != nil {
					fmt.Println("Eror unmarshalling cellvolt", result.ID)
				}

				errTMP := json.Unmarshal([]byte(result.TS), &tempDataProcessed)
				if errTMP != nil {
					fmt.Println("Eror unmarshalling tempsen ", result.ID)
				}

				dataToIns.SOC = append(dataToIns.SOC, result.SOC)
				dataToIns.SOH = append(dataToIns.SOH, result.SOH)
				dataToIns.CURR = append(dataToIns.CURR, result.CURR)
				dataToIns.PV = append(dataToIns.PV, result.PV)
				dataToIns.CYCLES = append(dataToIns.CYCLES, result.CYCLES)
				dataToIns.BRDTEMP = append(dataToIns.BRDTEMP, result.BRDTEMP)
				dataToIns.RPWR = append(dataToIns.RPWR, result.RPWR)
				dataToIns.STATUS = append(dataToIns.STATUS, result.STATUS)
				dataToIns.SOHWS = append(dataToIns.SOHWS, result.SOHWS)
				dataToIns.TIMESTAMP = append(dataToIns.TIMESTAMP, result.Timestamp)
				dataToIns.CELLVOLT = append(dataToIns.CELLVOLT, cellDataProcessed.CellVoltP)
				dataToIns.TEMPSEN = append(dataToIns.TEMPSEN, tempDataProcessed.TempSenP)

			}

			if err := cur.Err(); err != nil {
				log.Fatal(err)
			}

			cancel1()

			if len(dataToIns.SOC) > 0 {

				timeObj := time.Unix(int64(dataToIns.FROM/1000), 0)
				year := timeObj.Year()
				colProcessedData := client.Database("portal").Collection("processedAnalytics" + strconv.Itoa(year))

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
					"lastProcessedAnalytics": tempTo,
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

		result, err := colBatDataMain.DeleteMany(ctxDelete, filterDelete)

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
	fmt.Println("Main Script : ", currentTime)
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
