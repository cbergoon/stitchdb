package stitchdb

import (
	"fmt"
	"testing"
	"os"
)

func setup() {
	fmt.Println("Setup...")
}

func teardown() {
	fmt.Println("Teardown...")
}

func TestMain(m *testing.M) {
	setup()
	retCode := m.Run()
	teardown()

	os.Exit(retCode)
}