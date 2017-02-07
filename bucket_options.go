package main

type BucketOptions struct {
	system bool
	btdeg  int
	//On Eviction Function
	//On Invalidation Function
	//Is Geo?
	//On Geo Beacon is in Target Range Function
	//Geo Range is inclusive?
	//Time series?
	//Is System Bucket?
}

//Implement Bucket Options Here

func NewBucketOptions(options ...func(*BucketOptions) error) (*BucketOptions, error) {
	c := &BucketOptions{}
	for _, option := range options {
		err := option(c)
		if err != nil {
			return nil, ErrCouldNotCreateConfig
		}
	}
	return c, nil
}
