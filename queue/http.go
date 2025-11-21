package queue

import "net/http"

type HTTPHandler struct {
	q *RedisQueue
}

func NewHTTPHandler(q *RedisQueue) *HTTPHandler {
	return &HTTPHandler{
		q: q,
	}
}

func (h *HTTPHandler) JoinQueue(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	userKey := getOrSetUserKey(w, r)

	res, err := h.q.Join(ctx, userKey)
}
