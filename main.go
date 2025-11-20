package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	qs := NewQueueService(rdb, "black-friday-2025", 10)

	// /enter?user_id=xx
	http.HandleFunc("/enter", func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Query().Get("user_id")

		if userID == "" {
			http.Error(w, "user_id is required", http.StatusBadRequest)
			return
		}

		status, err := qs.Enter(userID)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeJSON(w, status)
	})

	// /admin?count=10
	http.HandleFunc("/admit", func(w http.ResponseWriter, r *http.Request) {
		countStr := r.URL.Query().Get("count")

		if countStr == "" {
			countStr = "1"
		}

		count, err := strconv.ParseInt(countStr, 10, 64)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := qs.Admin(count); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeJSON(w, map[string]any{"ok": true, "admitted": count})
	})

	addr := ":8080"
	log.Fatal(http.ListenAndServe(addr, nil))
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
