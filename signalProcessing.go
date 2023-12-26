package main

import (
	"context"
	"fmt"
	"log"
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

	cur0, err0 := colBatDataAll.Find(ctx4, bson.M{})
	if err0 != nil {
		log.Fatal(err0)
	}
	defer cur0.Close(context.TODO())

	for cur0.Next(context.TODO()) {
		var result BatDataAll
		err := cur0.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		batDataAllObjArray = append(batDataAllObjArray, result)
	}

	if err := cur0.Err(); err != nil {
		log.Fatal(err)
	}

	// -----------------------------------------------

	colBatDataMain := client.Database("portal").Collection("batDataSignal1")
	colProcessedData := client.Database("portal").Collection("processedSignal2023")
	initialTime := uint64(1609462861000)
	processTillTime := uint64(1703573690000)

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

			cur, err := colBatDataMain.Find(ctx1, filter1)
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

				_, err := colProcessedData.InsertOne(context.TODO(), dataToIns)
				if err != nil {
					log.Fatal(err)
				}
				// currentTime := time.Now()
				// fmt.Println(currentTime.Format("2006-01-02 15:04:05"), " => ", " Inserted ==>:", insertResult.InsertedID)
				fmt.Print("$")

			} else {
				// currentTime := time.Now()
				// fmt.Println(currentTime.Format("2006-01-02 15:04:05"), " => ", )
				fmt.Print(".")
			}

			//--------------------------------Saving timestamp for upgraded-------------------
			// filterBatData := bson.M{"bid": v.BID}
			// toUpdate := bson.M{
			// 	"$set": bson.M{
			// 		"lastProcessedAnalytics": tempTo,
			// 	},
			// }
			// lastProcessRes, lastProcessErr := colBatDataAll.UpdateOne(context.TODO(), filterBatData, toUpdate)
			// if lastProcessRes.MatchedCount == 0 || lastProcessErr != nil {

			// 	fmt.Println("Error occured while updating...")

			// }

			//-------------------------------------------------------------------------------
		}

		// -----------------  Delete old data of BID whose data is proccessed -------------

		// fmt.Println("Written data: ", v.BID)
		// filterDelete := bson.M{
		// 	"bid":       v.BID,
		// 	"timestamp": bson.M{"$lt": processTillTime},
		// }
		// ctxDelete, cancelDelete := context.WithCancel(context.Background())

		// result, err := colBatDataMain.DeleteMany(ctxDelete, filterDelete)
		// if err != nil {
		// 	log.Println(err)
		// }
		// cancelDelete()
		// fmt.Printf("Deleted %v documents of: %s\n", result.DeletedCount, v.BID)

		// -----------------------------------------------------------------------------------

	}

	err := client.Disconnect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection to MongoDB closed.")

}

func main() {

	fmt.Println("Script version: 12")
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	clientOptions := options.Client().ApplyURI("mongodb://administrator:%26%5E%23%25%21%2612dgf_%23%26@15.207.150.151:49125/?authSource=portal&readPreference=primary&directConnection=true&ssl=false")
	client, err := mongo.Connect(ctx, clientOptions)

	if err != nil {
		fmt.Println(err)
		return
	}

	handlePostRequest(client)
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
