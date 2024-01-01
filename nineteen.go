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
	LASTTIME uint64 `bson:"lastProcessedAnalytics, omitempty"`
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

	colBatDataMain := client.Database("portal").Collection("batDataSignal")
	initialTime := uint64(1703552461000)
	processTillTime := uint64(1703638861000)

	index := 10
	for i := 1034; i < len(batDataAllObjArray); i++ {
		v := batDataAllObjArray[i]

		filter1 := bson.M{"timestamp": bson.M{"$gte": initialTime, "$lte": processTillTime}, "bid": v.BID}
		ctx1, cancel1 := context.WithCancel(context.Background())

		cur, err := colBatDataMain.Find(ctx1, filter1)
		if err != nil {
			log.Println(err)
		}

		var dataToIns ProcessedData

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

			colProcessedData := client.Database("portal").Collection("processedSignal2023")

			filter1 := bson.M{"from": initialTime, "to": processTillTime, "bid": v.BID}

			ctxI, cancelI := context.WithCancel(context.Background())
			defer cancelI()

			var testResult ProcessedData
			colProcessedData.FindOne(ctxI, filter1).Decode(&testResult)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println(len(testResult.TIMESTAMP))

			testResult.AREACODE = append(testResult.AREACODE, dataToIns.AREACODE...)
			testResult.BER = append(testResult.BER, dataToIns.BER...)
			testResult.CELLID = append(testResult.CELLID, dataToIns.CELLID...)
			testResult.CCID = append(testResult.CCID, dataToIns.CCID...)
			testResult.NETWORKSTRENGTH = append(testResult.NETWORKSTRENGTH, dataToIns.NETWORKSTRENGTH...)
			testResult.SIGNALSTRENGTH = append(testResult.SIGNALSTRENGTH, dataToIns.SIGNALSTRENGTH...)
			testResult.SPNCODE = append(testResult.SPNCODE, dataToIns.SPNCODE...)
			testResult.TIMESTAMP = append(testResult.TIMESTAMP, dataToIns.TIMESTAMP...)

			fmt.Println(len(dataToIns.TIMESTAMP), len(testResult.TIMESTAMP))
			update := bson.M{"$set": testResult}
			_, err := colProcessedData.UpdateOne(context.Background(), filter1, update)
			if err != nil {
				fmt.Println("error")
			}

		} else {
			fmt.Print(".")
		}

		// -----------------  Delete old data of BID whose data is proccessed -------------
		index++
		fmt.Println("Written data: ", v.BID, index, processTillTime)

		filterDelete := bson.M{
			"bid":       v.BID,
			"timestamp": bson.M{"$lt": processTillTime},
		}
		ctxDelete, cancelDelete := context.WithCancel(context.Background())

		fmt.Println("Deleting with filter: ", filterDelete)
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
	fmt.Println("Connection to MongoDB closed.")

}

func main() {

	fmt.Println("Script version: 14")
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	clientOptions := options.Client().ApplyURI("mongodb://administrator:%26%5E%23%25%21%2612dgf_%23%26@15.207.150.151:49125/?authSource=portal&readPreference=primary&directConnection=true&ssl=false")
	client, err := mongo.Connect(ctx, clientOptions)

	if err != nil {
		fmt.Println(err)
		return
	}

	handlePostRequest(client)

	fmt.Println(time.Now())

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
