package copier

import (
	"container/list"
	"context"
	"io"

	"github.com/hashicorp/go-multierror"
)

type Group struct {
	list list.List
}

func (g *Group) Add(dst io.Writer, src io.Reader) Copier {
	return g.AddCopier(New(dst, src))
}

func (g *Group) AddCopier(copier Copier) Copier {
	e := &element{g, copier}
	go e.Copy()
	return e
}

func (g *Group) Wait(ctx context.Context) error {
	for {
		front := g.list.Front()
		if front == nil {
			return nil
		}

		err := front.Value.(Copier).Wait(ctx)
		if err != nil {
			return err
		}
	}
}

func (g *Group) Shutdown(ctx context.Context) error {
	count := 0
	front := g.list.Front()
	done := make(chan error)
	for front != nil {
		count++

		go func(c Copier) {
			done <- c.Shutdown(ctx)
		}(front.Value.(Copier))

		front = front.Next()
	}
	errs := make([]error, count)
	for idx := 0; idx < count; idx++ {
		errs[idx] = <-done
	}
	close(done)

	return multierror.Append(nil, errs...).ErrorOrNil()
}

type element struct {
	g *Group
	Copier
}

func (e *element) Copy() {
	elem := e.g.list.PushBack(e)
	defer e.g.list.Remove(elem)
	e.Copier.Copy()
}
