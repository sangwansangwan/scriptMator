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

type BatDataSignal struct {
	ID             string `bson:"_id,omitempty"`
	CellID         int64  `bson:"cellId,omitempty"`
	CcID           string `bson:"ccid,omitempty"`
	Mid            string `bson:"mid,omitempty"`
	NetworkStatus  int32  `bson:"networkStatus,omitempty"`
	SignalStrength int32  `bson:"signalStrength,omitempty"`
	Ber            int32  `bson:"ber,omitempty"`
	SpnCode        int64  `bson:"spnCode,omitempty"`
	Imei           string `bson:"imei,omitempty"`
	AreaCode       int32  `bson:"areaCode,omitempty"`
	Timestamp      int64  `bson:"timestamp"`
}

type BatDataAll struct {
	BID      string `bson:"bid"`
	LASTTIME uint64 `bson:"lastSignalProcessing, omitempty"`
}

type ProcessedData struct {
	CELLID          []int64
	CCID            []string
	BID             string
	MID             string
	NETWORKSTRENGTH []int32
	SIGNALSTRENGTH  []int32
	BER             []int32
	SPNCODE         []int64
	FROM            uint64
	TO              uint64
	IMEI            string
	AREACODE        []int32
	TIMESTAMP       []int64
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

	colBatSignal := client.Database("portal").Collection("batDataSignal")

	currentTime := time.Now()

	// fmt.Println(currentTime)
	initialTime := uint64(currentTime.Add(-15*24*time.Hour).UnixNano() / int64(time.Millisecond))
	// fmt.Println(initialTime)

	pastFiveDays := currentTime.Add(-4 * 24 * time.Hour)
	endTime := time.Date(pastFiveDays.Year(), pastFiveDays.Month(), pastFiveDays.Day(), 0, 0, 0, 0, time.UTC)

	processTillTime := uint64(endTime.UnixNano() / int64(time.Millisecond))

	// index := 0
	for _, v := range batDataAllObjArray {

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
			cur, err := colBatSignal.Find(ctx1, filter1, findOptionsMain)
			if err != nil {
				log.Println(err)
			}

			var dataToIns ProcessedData

			dataToIns.BID = v.BID
			dataToIns.FROM = tempFrom
			dataToIns.TO = tempTo

			for cur.Next(context.TODO()) {

				var result BatDataSignal

				err := cur.Decode(&result)
				if err != nil {
					log.Fatal(err)
				}

				dataToIns.AREACODE = append(dataToIns.AREACODE, result.AreaCode)
				dataToIns.BER = append(dataToIns.BER, result.Ber)
				dataToIns.CELLID = append(dataToIns.CELLID, result.CellID)
				dataToIns.CCID = append(dataToIns.CCID, result.CcID)
				dataToIns.NETWORKSTRENGTH = append(dataToIns.NETWORKSTRENGTH, result.NetworkStatus)
				dataToIns.SIGNALSTRENGTH = append(dataToIns.SIGNALSTRENGTH, result.SignalStrength)
				dataToIns.SPNCODE = append(dataToIns.SPNCODE, result.SpnCode)
				dataToIns.AREACODE = append(dataToIns.AREACODE, result.AreaCode)
				dataToIns.TIMESTAMP = append(dataToIns.TIMESTAMP, result.Timestamp)
				dataToIns.MID = result.Mid
				dataToIns.IMEI = result.Imei
			}

			if err := cur.Err(); err != nil {
				log.Fatal(err)
			}

			cancel1()

			if len(dataToIns.TIMESTAMP) > 0 {

				timeObj := time.Unix(int64(dataToIns.FROM/1000), 0)
				year := timeObj.Year()
				colProcessedData := client.Database("portal").Collection("processedSignal" + strconv.Itoa(year))

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
					"lastSignalProcessing": tempTo,
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

		result, err := colBatSignal.DeleteMany(ctxDelete, filterDelete)

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

	currentStartTime := time.Now()
	fmt.Println("Signal Script : ", currentStartTime)
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
