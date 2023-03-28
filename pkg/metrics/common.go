package metrics

const initPortfolioLoadTimeBucket = 10

func getLoadTimeBucket() []float64 {
	var buckets []float64
	for i := initPortfolioLoadTimeBucket; i <= 200; i += 10 {
		buckets = append(buckets, float64(i))
	}
	for i := 210; i <= 4500; i += 100 {
		buckets = append(buckets, float64(i))
	}
	for i := 4600; i <= 15000; i += 500 {
		buckets = append(buckets, float64(i))
	}
	return buckets
}
