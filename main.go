package main

import (
	"context"
	"encoding/json"
	"github.com/go-chi/chi"
	"github.com/stellar/go/clients/horizon"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var (
	mutex          sync.Mutex
	sequenceNumber int32
	memory         []pair
)

const horizonAddress = "http://127.0.0.1:8000"
const port = ":3000"
const offerOperationId = 3

type pair struct {
	Time  time.Time     `json:"time"`
	Offer horizon.Offer `json:"offer"`
}

type LedgerPage struct {
	Links struct {
		Self horizon.Link `json:"self"`
		Next horizon.Link `json:"next"`
		Prev horizon.Link `json:"prev"`
	} `json:"_links"`
	Embedded struct {
		Records []horizon.Ledger `json:"records"`
	} `json:"_embedded"`
}

func dbGetOffer(start, end string) []pair {
	var result []pair

	s, err := time.Parse(time.RFC3339, end)
	if err != nil {
		println(err)
	}
	e, err := time.Parse(time.RFC3339, start)
	if err != nil {
		println(err)
	}

	mutex.Lock()
	for _, v := range memory {
		if s.After(v.Time) && e.Before(v.Time) {
			result = append(result, v)
		}
	}
	mutex.Unlock()

	return result
}

func offerCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var offers []pair

		if start, end := chi.URLParam(r, "start"), chi.URLParam(r, "end"); start != "" && end != "" {
			offers = dbGetOffer(start, end)
		} else {
			js, err := json.Marshal([]byte("Not found"))

			if err != nil {
				println(err)
			}

			w.Write(js)
			return
		}
		ctx := context.WithValue(r.Context(), "offers", offers)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func GetOffer(w http.ResponseWriter, r *http.Request) {
	offer := r.Context().Value("offers").([]pair)
	js, err := json.Marshal(offer)
	if err != nil {
		panic(err)
	}
	w.Write(js)
}

func getCursor(client *horizon.Client) string {
	resp, err := client.HTTP.Get(client.URL + "/ledgers?limit=1")
	if err != nil {
		panic(err)
	}
	buffer, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	var page LedgerPage
	err = json.Unmarshal(buffer, &page)
	if err != nil {
		panic(err)
	}

	return page.Embedded.Records[0].PT
}

func fetchLedgers() {

	client := &horizon.Client{
		HTTP: http.DefaultClient,
		URL:  horizonAddress,
	}

	pagingToken := getCursor(client)

	cur := horizon.Cursor(pagingToken)

	client.StreamLedgers(context.TODO(),
		&cur,
		func(ledger horizon.Ledger) {
			mutex.Lock()
			sequenceNumber = ledger.Sequence
			mutex.Unlock()

			if ledger.OperationCount != 0 {
				resp, err := client.HTTP.Get(ledger.Links.Operations.Href)
				if err != nil {
					println(err)
				}
				buffer, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					println(err)
				}

				var op horizon.OffersPage
				var OpType struct {
					TypeI int `json:"type_i"`
				}
				err = json.Unmarshal(buffer, OpType)

				if err != nil {
					println(err)
				}

				if OpType.TypeI != offerOperationId {
					return
				}

				err = json.Unmarshal(buffer, &op)
				if err != nil {
					println(err)
				}
				mutex.Lock()
				for _, offer := range op.Embedded.Records {
					memory = append(memory, pair{Time: ledger.ClosedAt, Offer: offer})
				}
				mutex.Unlock()
			} else {
				return
			}
		})
}

func main() {
	go fetchLedgers()

	router := chi.NewRouter()
	router.Get("/ledger", func(w http.ResponseWriter, r *http.Request) {
		js, err := json.Marshal([]byte(strconv.FormatInt(int64(sequenceNumber), 10)))

		if err != nil {
			println(err)
		}

		w.Write(js)
	})

	router.Get("/offers{?start,end}", func(w http.ResponseWriter, r *http.Request) {
		router.With(offerCtx)
		router.Get("/", GetOffer)
	})

	http.ListenAndServe(port, router)

}
