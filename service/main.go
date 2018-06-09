package main

import (
	elastic "gopkg.in/olivere/elastic.v3"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"reflect"
	"github.com/pborman/uuid"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
}

const (
	INDEX = "around"
	TYPE = "post"
	DISTANCE = "200km"
	// Needs to update
	// PROJECT_ID = "around-dongxin2"
	// BT_INSTANCE = "around-post"
	// Needs to update this URL if you deploy it to cloud
	ES_URL = "http://35.193.229.151:9200"

)

func main() {
	// Create a Client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}
	// Use the IndexExists service to check if a specific index exists
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new Index
		mapping := `{
			"mappings": {
				"post":{
					"properties":{
						"location":{
							"type":"geo_point"
						}
					}
				}
			}
			}`
			_, err := client.CreateIndex(INDEX).Body(mapping).Do()
			if err != nil {
				panic(err)
			}
	}
	fmt.Println("started-servie")
	http.HandleFunc("/post", handlerPost)
	http.HandleFunc("/search", handlerSearch)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// r must be jSON Arrya
//{
// 	"user_name" : "John",
// 	"message" : "Test",
// 	"location" : {
// 		"lat" : 37,
// 		"lon" : -120
// 	}
//}
func handlerPost(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one post request.")

	decoder := json.NewDecoder(r.Body)
	var p Post
	if err := decoder.Decode(&p); err != nil {
		// ; represent two instr lines;
		// in this if statement, it has two lines, one for initialization,
		// one for comparison
		// scope, this err is only for this if statement
		panic(err)
		return
	}
	id := uuid.New()
	// Save to ES
	saveToES(&p, id)
	// w is the value our server send to browser
	fmt.Fprintf(w, "Post received: %s\n", p.Message)
}

func saveToES(p *Post, id string) {
	// Create a client
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Save it to Index
	_, err = es_client.Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh(true).
		Do()
		if err != nil {
			panic(err)
			return
		}
		fmt.Printf("Post is saved to Index: %s\n", p.Message)
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search.")
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)

	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	fmt.Printf("Search received: %f %f %s\n", lat, lon, ran)

	//Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// //Return a fake post
	// p := &Post{
	// 	User:    "dongxin2",
	// 	Message: "Loving Taylor",
	// 	Location: Location{
	// 		Lat: lat,
	// 		Lon: lon,
	// 	},
	// }

	// Create a client
	client, err = elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Define geo distance query as specific in
	// https://www.elastic.co/guide/en/elasticsearch/reference/5.2/query-dsl-geo-distance-query.html
	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)

	// Some delay may range from seconds to minutes. So if you don't get enough results. Try it later.
	searchResult, err := client.Search().
		Index(INDEX).
		Query(q).
		Pretty(true).
		Do()
	if err != nil {
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
  // and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	// TotalHits is another convenience function that works even when something goes wrong
	fmt.Printf("Found a total of %d post\n", searchResult.TotalHits())

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you dont need to check for nil values in the response
	// However it ignoers errors in serialization
	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) {
		p := item.(Post)	//p = (Post) item
		fmt.Printf("Post by %s: %s at alt %v and lon %v\n", p.User, p.Message,
			p.Location.Lat, p.Location.Lon)
		// TODO: Perform filtering based on keywords such as web spam
		// I think we can filter p.Message.
		ps = append(ps, p)

	}
	// convert Go data struct to json string
	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "")
	w.Write(js)
}
