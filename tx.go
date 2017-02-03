package main

type RWMode int

const (
	MODE_READ RWMode = iota
	MODE_READ_WRITE
)

type RbCtx struct {
	//Everything that we may want to roll back.
}

type Tx struct {
	mode  RWMode
	db    *StitchDB
	rbctx *RbCtx
}

func NewTx() (*Tx, error) {
	return nil, nil
}

func (t *Tx) RollbackTx() error {
	return nil
}

func (t *Tx) CommitTx() error {
	return nil
}

func (t *Tx) lock() {
	return
}

func (t *Tx) unlock() {
	return
}

func (t *Tx) Ascend() error {
	return nil
}

func (t *Tx) Descend() error {
	return nil
}

func (t *Tx) AscendIndex() error {
	return nil
}

func (t *Tx) DescendIndex() error {
	return nil
}

func (t *Tx) AscendIndexRange() error {
	return nil
}

func (t *Tx) DescendIndexRange() error {
	return nil
}

func (t *Tx) AscendIndexLessThan() error {
	return nil
}

func (t *Tx) DescendIndexLessOrGreater() error {
	return nil
}

func (t *Tx) AscendIndexLessOrGreater() error {
	return nil
}

func (t *Tx) DescendIndexLessThan() error {
	return nil
}

func (t *Tx) Get() error {
	return nil
}

func (t *Tx) Set() error {
	return nil
}

func (t *Tx) Delete() error {
	return nil
}

func (t *Tx) CreateIndex() error {
	return nil
}

func (t *Tx) DropIndex() error {
	return nil
}

func (t *Tx) Indexes() error {
	return nil
}
