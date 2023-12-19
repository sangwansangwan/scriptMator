package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

	cur0, err0 := colBatDataAll.Find(ctx4, bson.M{})
	if err0 != nil {
		log.Fatal(err0)
	}
	defer cur0.Close(context.TODO())

	// Loop through the cursor and decode each document
	for cur0.Next(context.TODO()) {
		var result BatDataAll
		err := cur0.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		// Print the result (you can replace this with your desired logic)
		batDataAllObjArray = append(batDataAllObjArray, result)
	}

	if err := cur0.Err(); err != nil {
		log.Fatal(err)
	}

	// -----------------------------------------------

	colBatDataMain := client.Database("portal").Collection("batDataMain")
	colProcessedData := client.Database("portal").Collection("processedAnalytics")
	// myGlobalIP := []string{"A6FFBE11"}
	batIndex := 0
	for _, v := range batDataAllObjArray {
		globalTimeStart := uint64(1609462861000)
		if v.LASTTIME != 0 {
			globalTimeStart = v.LASTTIME
		}
		for globalTimeStart < 1690196118000 {

			tempFrom := globalTimeStart

			tempTo := globalTimeStart + 86400000
			globalTimeStart = globalTimeStart + 86400000

			filter1 := bson.M{"timestamp": bson.M{"$gte": tempFrom, "$lt": tempTo}, "bid": v.BID}
			ctx1, cancel1 := context.WithCancel(context.Background())
			defer cancel1()

			cur, err := colBatDataMain.Find(ctx1, filter1)
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
					//log.Fatalf("Error unmarshalling the JSON: %s", err)

					fmt.Println("Eror in cva json ", result.ID)
				}

				errTMP := json.Unmarshal([]byte(result.TS), &tempDataProcessed)
				if errTMP != nil {
					//log.Fatalf("Error unmarshalling the JSON: %s", err)
					fmt.Println("Eror in tac json ", result.ID)
				}

				// if len(tempDataProcessed.TempSenP) < 1 || len(cellDataProcessed.CellVoltP) < 1 || len(tempDataProcessed.TempSenP) > 4 || len(cellDataProcessed.CellVoltP) > 17 {
				// 	continue
				// }

				// if firstTime == 1 && (len(tempDataProcessed.TempSenP) != tempsenLen || len(cellDataProcessed.CellVoltP) != cellVolLen) {
				// 	fmt.Println(tempDataProcessed.TempSenP, tempsenLen)
				// 	fmt.Println("Skipping lower condition")
				// 	continue
				// }

				// if firstTime == 0 {
				// 	firstTime = 1
				// }

				// if len(dataToIns.TEMPSEN) > 0 {
				// 	index := len(dataToIns.TEMPSEN[0])
				// 	if len(tempDataProcessed.TempSenP) < len(dataToIns.TEMPSEN) {
				// 		difference := len(dataToIns.TEMPSEN) - len(tempDataProcessed.TempSenP)
				// 		for i := 0; i < difference; i++ {
				// 			tempDataProcessed.TempSenP = append(tempDataProcessed.TempSenP, 0)
				// 		}
				// 	}
				// 	dataToIns.TEMPSEN = insertColumn(dataToIns.TEMPSEN, tempDataProcessed.TempSenP, index)

				// } else {
				// 	index := 0
				// 	tempTempSen := make([][]int, tempsenLen)
				// 	dataToIns.TEMPSEN = insertColumn(tempTempSen, tempDataProcessed.TempSenP, index)
				// }

				// if len(dataToIns.CELLVOLT) > 0 {
				// 	index := len(dataToIns.CELLVOLT[0])
				// 	if len(cellDataProcessed.CellVoltP) < len(dataToIns.CELLVOLT) {
				// 		difference := len(dataToIns.CELLVOLT) - len(cellDataProcessed.CellVoltP)
				// 		for i := 0; i < difference; i++ {
				// 			cellDataProcessed.CellVoltP = append(cellDataProcessed.CellVoltP, 0)
				// 		}
				// 	}
				// 	dataToIns.CELLVOLT = insertColumn(dataToIns.CELLVOLT, cellDataProcessed.CellVoltP, index)

				// } else {
				// 	index := 0
				// 	tempCellSen := make([][]int, cellVolLen)
				// 	dataToIns.CELLVOLT = insertColumn(tempCellSen, cellDataProcessed.CellVoltP, index)
				// }

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

			if len(dataToIns.SOC) > 0 {

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

		batIndex++
		fmt.Println("Written data: ", v.BID, "Index", batIndex)
		// To Do Have to delete data of that battery before the processsed date

	}

	err := client.Disconnect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection to MongoDB closed.")

}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	clientOptions := options.Client().ApplyURI("mongodb://administrator:%26%5E%23%25%21%2612dgf_%23%26@15.207.150.151:49125/?authSource=portal&readPreference=primary&directConnection=true&ssl=false")
	client, err := mongo.Connect(ctx, clientOptions)

	if err != nil {
		fmt.Println(err)
		return
	}
	//router := mux.NewRouter()

	//reader := bufio.NewReader(os.Stdin)

	//fmt.Print("Do you want to proceed? Enter 'Y' to continue: ")
	//input, _ := reader.ReadString('\n')
	//input = strings.TrimSpace(input)

	handlePostRequest(client)
	// if strings.ToLower(input) == "y" {
	// 	handlePostRequest()
	// } else {
	// 	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// 	defer cancel()
	// 	err := client.Disconnect(ctx)
	// 	if err != nil {
	// 		log.Fatalf("Error disconnecting from MongoDB: %v", err)
	// 	}
	// 	fmt.Println("Connection to MongoDB closed.")
	// }

	//http.ListenAndServe(":8000", router)
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
