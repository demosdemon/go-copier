package copier

import (
	"context"
	"io"
	"sync"

	"github.com/hashicorp/go-multierror"
)

type (
	Copier interface {
		CloseReader
		CloseWriter
		GracefulStopper

		Written() int64
		Err() error

		Copy()
	}

	CloseReader interface {
		CloseRead() error
	}

	CloseWriter interface {
		CloseWrite() error
	}

	GracefulStopper interface {
		Waiter
		Shutdown(ctx context.Context) error
	}

	Waiter interface {
		Wait(ctx context.Context) error
	}
)

func New(dst io.Writer, src io.Reader) Copier {
	return &instance{
		src: src,
		dst: dst,
	}
}

type instance struct {
	mu   sync.Mutex
	src  io.Reader
	dst  io.Writer
	done chan struct{}

	written int64
	error   error
}

func (c *instance) Close() error {
	return multierror.Append(nil, c.CloseRead(), c.CloseWrite()).ErrorOrNil()
}

func (c *instance) Shutdown(ctx context.Context) error {
	errCloseRead := c.CloseRead()

	errWait := c.Wait(ctx)
	ctxErr := ctx.Err()

	if ctxErr != nil {
		return ctxErr
	}

	errCloseWrite := c.CloseWrite()

	return multierror.Append(errCloseRead, errWait, errCloseWrite).ErrorOrNil()
}

func (c *instance) CloseRead() error {
	c.mu.Lock()
	src := c.src
	c.src = nil
	c.mu.Unlock()

	if src == nil {
		return nil
	}

	if v, ok := src.(CloseReader); ok {
		return v.CloseRead()
	}

	if v, ok := src.(io.Closer); ok {
		return v.Close()
	}

	return nil
}

func (c *instance) CloseWrite() error {
	c.mu.Lock()
	dst := c.dst
	c.dst = nil
	c.mu.Unlock()

	if dst == nil {
		return nil
	}

	if v, ok := dst.(CloseWriter); ok {
		return v.CloseWrite()
	}

	if v, ok := dst.(io.Closer); ok {
		return v.Close()
	}

	return nil
}

func (c *instance) Written() int64 {
	return c.written
}

func (c *instance) Err() error {
	return c.error
}

func (c *instance) Copy() {
	c.mu.Lock()
	done := c.done
	if done != nil {
		c.mu.Unlock()
		<-done
		return
	}

	done = make(chan struct{})
	c.done = done
	c.mu.Unlock()

	c.written, c.error = io.Copy(c.dst, c.src)

	if c.error != nil {
		c.error = c.Close()
	}

	close(done)
}

func (c *instance) Wait(ctx context.Context) error {
	c.mu.Lock()
	done := c.done
	c.mu.Unlock()

	if done == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}
