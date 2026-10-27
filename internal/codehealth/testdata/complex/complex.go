// Package complex holds one function just over the complexity cap and one at it.
package complex

// TooComplex has 40 branches: complexity 41.
func TooComplex(x int) int {
	if x == 0 {
		x++
	}
	if x == 1 {
		x++
	}
	if x == 2 {
		x++
	}
	if x == 3 {
		x++
	}
	if x == 4 {
		x++
	}
	if x == 5 {
		x++
	}
	if x == 6 {
		x++
	}
	if x == 7 {
		x++
	}
	if x == 8 {
		x++
	}
	if x == 9 {
		x++
	}
	if x == 10 {
		x++
	}
	if x == 11 {
		x++
	}
	if x == 12 {
		x++
	}
	if x == 13 {
		x++
	}
	if x == 14 {
		x++
	}
	if x == 15 {
		x++
	}
	if x == 16 {
		x++
	}
	if x == 17 {
		x++
	}
	if x == 18 {
		x++
	}
	if x == 19 {
		x++
	}
	if x == 20 {
		x++
	}
	if x == 21 {
		x++
	}
	if x == 22 {
		x++
	}
	if x == 23 {
		x++
	}
	if x == 24 {
		x++
	}
	if x == 25 {
		x++
	}
	if x == 26 {
		x++
	}
	if x == 27 {
		x++
	}
	if x == 28 {
		x++
	}
	if x == 29 {
		x++
	}
	if x == 30 {
		x++
	}
	if x == 31 {
		x++
	}
	if x == 32 {
		x++
	}
	if x == 33 {
		x++
	}
	if x == 34 {
		x++
	}
	if x == 35 {
		x++
	}
	if x == 36 {
		x++
	}
	if x == 37 {
		x++
	}
	if x == 38 {
		x++
	}
	if x == 39 {
		x++
	}
	return x
}

// AtCap has 39 branches: complexity 40, the most allowed.
func AtCap(x int) int {
	if x == 0 {
		x++
	}
	if x == 1 {
		x++
	}
	if x == 2 {
		x++
	}
	if x == 3 {
		x++
	}
	if x == 4 {
		x++
	}
	if x == 5 {
		x++
	}
	if x == 6 {
		x++
	}
	if x == 7 {
		x++
	}
	if x == 8 {
		x++
	}
	if x == 9 {
		x++
	}
	if x == 10 {
		x++
	}
	if x == 11 {
		x++
	}
	if x == 12 {
		x++
	}
	if x == 13 {
		x++
	}
	if x == 14 {
		x++
	}
	if x == 15 {
		x++
	}
	if x == 16 {
		x++
	}
	if x == 17 {
		x++
	}
	if x == 18 {
		x++
	}
	if x == 19 {
		x++
	}
	if x == 20 {
		x++
	}
	if x == 21 {
		x++
	}
	if x == 22 {
		x++
	}
	if x == 23 {
		x++
	}
	if x == 24 {
		x++
	}
	if x == 25 {
		x++
	}
	if x == 26 {
		x++
	}
	if x == 27 {
		x++
	}
	if x == 28 {
		x++
	}
	if x == 29 {
		x++
	}
	if x == 30 {
		x++
	}
	if x == 31 {
		x++
	}
	if x == 32 {
		x++
	}
	if x == 33 {
		x++
	}
	if x == 34 {
		x++
	}
	if x == 35 {
		x++
	}
	if x == 36 {
		x++
	}
	if x == 37 {
		x++
	}
	if x == 38 {
		x++
	}
	return x
}

type runner struct{}

// Branchy counts each kind of branch once: if, for, range, case, select case, && and ||.
func (r *runner) Branchy(xs []int, ch chan int) int {
	n := 0
	if len(xs) > 0 && xs[0] > 0 || n > 0 {
		n++
	}
	for i := 0; i < 3; i++ {
		n++
	}
	for range xs {
		n++
	}
	switch n {
	case 1:
		n++
	default:
	}
	select {
	case v := <-ch:
		n += v
	default:
	}
	f := func() {
		if n > 9 {
			n--
		}
	}
	f()
	return n
}
