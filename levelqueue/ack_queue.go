package levelqueue

type inAckQueue []*Message

func newInAckQueue(capacity int) inAckQueue {
	return make(inAckQueue, 0, capacity)
}

func (q inAckQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *inAckQueue) Push(x *Message) {
	n := len(*q)
	c := cap(*q)
	if n+1 > c {
		npq := make(inAckQueue, n, c*2)
		copy(npq, *q)
		*q = npq
	}
	*q = (*q)[0 : n+1]
	x.index = n
	(*q)[n] = x
	q.up(n)
}

func (q *inAckQueue) Pop() *Message {
	n := len(*q)
	c := cap(*q)
	q.Swap(0, n-1)
	q.down(0, n-1)
	if n < (c/2) && c > 25 {
		npq := make(inAckQueue, n, c/2)
		copy(npq, *q)
		*q = npq
	}
	x := (*q)[n-1]
	x.index = -1
	*q = (*q)[0 : n-1]
	return x
}

func (q *inAckQueue) Remove(i int) *Message {
	n := len(*q)
	if n-1 != i {
		q.Swap(i, n-1)
		q.down(i, n-1)
		q.up(i)
	}
	x := (*q)[n-1]
	x.index = -1
	*q = (*q)[0 : n-1]
	return x
}

func (q *inAckQueue) PeekAndShift(max int64) (*Message, int64) {
	if len(*q) == 0 {
		return nil, 0
	}

	x := (*q)[0]
	if x.pri > max {
		return nil, x.pri - max
	}
	q.Pop()

	return x, 0
}

func (q *inAckQueue) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || (*q)[j].pri >= (*q)[i].pri {
			break
		}
		q.Swap(i, j)
		j = i
	}
}

func (q *inAckQueue) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && (*q)[j1].pri >= (*q)[j2].pri {
			j = j2
		}
		if (*q)[j].pri >= (*q)[i].pri {
			break
		}
		q.Swap(i, j)
		i = j
	}
}
