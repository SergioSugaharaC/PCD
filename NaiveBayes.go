package main

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"sync"
	"time"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"strings"

)

// Definicion de estructura de datos
type variable map[string]float64
type field map[string]variable

var fields = make(field)

// Definicion de wait group
var waitGroup sync.WaitGroup

// Definicion de variables de control
var dt [][]string   // almacenamiento de dataset
var ys, ns float64  // conteo de datos de cada clase (para cada histograma)
var ry, rn float64  // probabilidad final Yes y No
var yes, no float64 // probabilidad de la tupla a clasificar

// lectura del dataset mediante url
func readCSVFromURL(url string) ([][]string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	reader := csv.NewReader(resp.Body)
	reader.Comma = ','
	data, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	return data, nil
}

// conteo de histograma
func countTimes(row []string) {
	var i int
	var val string
	defer waitGroup.Done()
	for i = range row {
		if i == len(row)-1 {
			break
		}
		val = row[i]
		vars, exists := fields[val]
		if exists {
			if row[len(row)-1] == "yes" {
				vars["yes"] = vars["yes"] + 1
				ys++
			}
			if row[len(row)-1] == "no" {
				vars["no"] = vars["no"] + 1
				ns++
			}
		}
		if exists == false {
			n, y := 0.0, 0.0
			if row[len(row)-1] == "yes" {
				y = 1
				ys++
			}
			if row[len(row)-1] == "no" {
				n = 0
				ns++
			}
			v := variable{"yes": y, "no": n}
			fields[val] = v
		}
	}
	if row[len(row)-1] == "yes" {
		ry++
	}
	if row[len(row)-1] == "no" {
		rn++
	}
}

// calculo de probabilidades de datos
func calcProbs(alpha float64) {
	ys += 10
	ns += 10
	for _, v := range fields {
		v["yes"] = (v["yes"] + alpha) / ys
		v["no"] = (v["no"] + alpha) / ns
	}
	total := ry + rn
	ry = ry / total
	rn = rn / total
}

// funcion clasificadora
func classifier(row []string, res string) {
	prob := 1.0
	defer waitGroup.Done()
	for i := range row {
		val := row[i]
		vars, exists := fields[val]
		if exists {
			prob *= vars[res]
		}
	}
	if res == "yes" {
		yes = prob * ry
	}
	if res == "no" {
		no = prob * rn
	}
}

func retrain(_datas [][]string){//
	waitGroup.Add(len(_datas) - 1)
	for idx, row := range _datas {
		// skip header
		if idx == 0 {
			continue
		}
		
		go countTimes(row)
		time.Sleep(time.Millisecond * 5)
	}
	// suma de alpha y calculo de probabilidades
	waitGroup.Wait()
	calcProbs(1.0)

	fmt.Println("trained")
}

func testing(){
	// clasificacion de arreglo inexistente en el dataset
	test := []string{"rainy", "hot", "high", "TRUE"}
	waitGroup.Add(2)
	go classifier(test, "yes")
	go classifier(test, "no")
	waitGroup.Wait()

	// impresion de resultados
	fmt.Println("Yes, with: ", yes)
	fmt.Println("No, with:  ", no)
	if yes > no {
		fmt.Println("Classified as Yes")
	} else {
		fmt.Println("Classified as No")
	}
}

// funcion principal
func main() {
	var option string
	var train, test int
	local := os.Args[1]
	remotes := os.Args[2:]
	ch := make(chan Msg)

	// creacion del dataset y conteo de ocurrencias
	datas, err := readCSVFromURL("https://gist.githubusercontent.com/bigsnarfdude/515849391ad37fe593997fe0db98afaa/raw/f663366d17b7d05de61a145bbce7b2b961b3b07f/weather.csv")
	if err != nil {
		panic(err)
	}
	fmt.Println("dataset imported")
	//dt = datas
	go server(local, remotes, datas, ch)
	
	for{
		fmt.Print("Your option:")
		fmt.Scanf("%s", &option)
		sendAll(option, local, remotes)

		if option == "train" {
			train = 1
		}
		if option == "test" {
			test = 1
		}
		//if option == "add" {
		//	add = 1
		//} 
		
		for range remotes {
			msg := <-ch
			if msg.Option == "train" {
				train++
			}
			if msg.Option == "test" {
				test++
			}
			//if msg.Option == "add" {
			//	add++
			//}
		}
		if train > test {
			fmt.Println("Training Time...")
			retrain(datas)
			//train = 0
			//test = 0
			fmt.Println(train, test)
			//TODO: count rows and calc probs
		} else {
			fmt.Println("classifying...")
			testing()
			//train = 0
			//test = 0
			fmt.Println(train, test)
			//TODO: send new rows and classify
		}
	}
}

//Msg lorem ipsum
type Msg struct {
	Addr   string `json:"addr"`
	Option string `json:"option"`
}

func server(local string, remotes []string, _datas [][]string, ch chan Msg) {
	if ln, err := net.Listen("tcp", local); err == nil {
		defer ln.Close()
		fmt.Printf("Listening on %s\n", local)
		for {
			if conn, err := ln.Accept(); err == nil {
				fmt.Printf("Connection accepted from %s\n", conn.RemoteAddr())
				go handle(conn, local, remotes, _datas, ch)
			}
		}
	}
}

func handle(conn net.Conn, local string, remotes []string, _datas [][]string, ch chan Msg) {
	defer conn.Close()
	dec := json.NewDecoder(conn)
	var msg Msg
	if err := dec.Decode(&msg); err == nil {
		switch msg.Option {
		case "train":
			fmt.Printf("Message: %v\n", msg)
			dSize := len(_datas) / (len(remotes) + 1)
            fmt.Print(dSize, len(_datas), len(remotes))
            res1 := strings.SplitAfterN(local, "localhost:800", 2)
            res,_ := strconv.Atoi(res1[1])
            fmt.Print(_datas[(res)*dSize : (res+1)*dSize][:])
            if res < len(remotes) {
                go retrain(_datas[(res)*dSize : (res+1)*dSize][:])
            } else {
                go retrain(_datas[(res)*dSize : len(_datas)-1][:])
            }
			ch <- msg
		case "test":
			fmt.Printf("Message: %v\n", msg)
			ch <- msg
		}
		
	}
}

func sendAll(option, local string, remotes []string) {
	for _, remote := range remotes {
		send(local, remote, option)
	}
}

func send(local, remote, option string) {
	if conn, err := net.Dial("tcp", remote); err == nil {
		enc := json.NewEncoder(conn)
		if err := enc.Encode(Msg{local, option}); err == nil {
			fmt.Printf("Sending %s to %s\n", option, remote)
		}
	}
}
