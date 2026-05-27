package counter

import (
	"io"
	"net"

	"github.com/sagernet/sing/common/bufio"

	"github.com/sagernet/sing/common/buf"

	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/common/network"
)

type ConnCounter struct {
	network.ExtendedConn
	storage   *TrafficStorage
	readFunc  network.CountFunc
	writeFunc network.CountFunc
}

func NewConnCounter(conn net.Conn, s *TrafficStorage) net.Conn {
	return &ConnCounter{
		ExtendedConn: bufio.NewExtendedConn(conn),
		storage:      s,
		readFunc: func(n int64) {
			s.UpCounter.Add(n)
		},
		writeFunc: func(n int64) {
			s.DownCounter.Add(n)
		},
	}
}

func (c *ConnCounter) Read(b []byte) (n int, err error) {
	n, err = c.ExtendedConn.Read(b)
	c.storage.UpCounter.Store(int64(n))
	return
}

func (c *ConnCounter) Write(b []byte) (n int, err error) {
	n, err = c.ExtendedConn.Write(b)
	c.storage.DownCounter.Store(int64(n))
	return
}

func (c *ConnCounter) ReadBuffer(buffer *buf.Buffer) error {
	err := c.ExtendedConn.ReadBuffer(buffer)
	if err != nil {
		return err
	}
	if buffer.Len() > 0 {
		c.storage.UpCounter.Add(int64(buffer.Len()))
	}
	return nil
}

func (c *ConnCounter) WriteBuffer(buffer *buf.Buffer) error {
	dataLen := int64(buffer.Len())
	err := c.ExtendedConn.WriteBuffer(buffer)
	if err != nil {
		return err
	}
	if dataLen > 0 {
		c.storage.DownCounter.Add(dataLen)
	}
	return nil
}

func (c *ConnCounter) UnwrapReader() (io.Reader, []network.CountFunc) {
	return c.ExtendedConn, []network.CountFunc{
		c.readFunc,
	}
}

func (c *ConnCounter) UnwrapWriter() (io.Writer, []network.CountFunc) {
	return c.ExtendedConn, []network.CountFunc{
		c.writeFunc,
	}
}

func (c *ConnCounter) Upstream() any {
	return c.ExtendedConn
}

type ConnMultiCounter struct {
	network.ExtendedConn
	storages  []*TrafficStorage
	readFunc  network.CountFunc
	writeFunc network.CountFunc
}

func NewConnMultiCounter(conn net.Conn, storages ...*TrafficStorage) net.Conn {
	counters := compactStorages(storages)
	return &ConnMultiCounter{
		ExtendedConn: bufio.NewExtendedConn(conn),
		storages:     counters,
		readFunc: func(n int64) {
			addUp(counters, n)
		},
		writeFunc: func(n int64) {
			addDown(counters, n)
		},
	}
}

func (c *ConnMultiCounter) Read(b []byte) (n int, err error) {
	n, err = c.ExtendedConn.Read(b)
	if n > 0 {
		addUp(c.storages, int64(n))
	}
	return
}

func (c *ConnMultiCounter) Write(b []byte) (n int, err error) {
	n, err = c.ExtendedConn.Write(b)
	if n > 0 {
		addDown(c.storages, int64(n))
	}
	return
}

func (c *ConnMultiCounter) ReadBuffer(buffer *buf.Buffer) error {
	err := c.ExtendedConn.ReadBuffer(buffer)
	if err != nil {
		return err
	}
	if buffer.Len() > 0 {
		addUp(c.storages, int64(buffer.Len()))
	}
	return nil
}

func (c *ConnMultiCounter) WriteBuffer(buffer *buf.Buffer) error {
	dataLen := int64(buffer.Len())
	err := c.ExtendedConn.WriteBuffer(buffer)
	if err != nil {
		return err
	}
	if dataLen > 0 {
		addDown(c.storages, dataLen)
	}
	return nil
}

func (c *ConnMultiCounter) UnwrapReader() (io.Reader, []network.CountFunc) {
	return c.ExtendedConn, []network.CountFunc{
		c.readFunc,
	}
}

func (c *ConnMultiCounter) UnwrapWriter() (io.Writer, []network.CountFunc) {
	return c.ExtendedConn, []network.CountFunc{
		c.writeFunc,
	}
}

func (c *ConnMultiCounter) Upstream() any {
	return c.ExtendedConn
}

type PacketConnCounter struct {
	network.PacketConn
	storage   *TrafficStorage
	readFunc  network.CountFunc
	writeFunc network.CountFunc
}

func NewPacketConnCounter(conn network.PacketConn, s *TrafficStorage) network.PacketConn {
	return &PacketConnCounter{
		PacketConn: conn,
		storage:    s,
		readFunc: func(n int64) {
			s.UpCounter.Add(n)
		},
		writeFunc: func(n int64) {
			s.DownCounter.Add(n)
		},
	}
}

func (p *PacketConnCounter) ReadPacket(buff *buf.Buffer) (destination M.Socksaddr, err error) {
	destination, err = p.PacketConn.ReadPacket(buff)
	if err != nil {
		return
	}
	p.storage.UpCounter.Add(int64(buff.Len()))
	return
}

func (p *PacketConnCounter) WritePacket(buff *buf.Buffer, destination M.Socksaddr) (err error) {
	n := buff.Len()
	err = p.PacketConn.WritePacket(buff, destination)
	if err != nil {
		return
	}
	if n > 0 {
		p.storage.DownCounter.Add(int64(n))
	}
	return
}

func (p *PacketConnCounter) UnwrapPacketReader() (network.PacketReader, []network.CountFunc) {
	return p.PacketConn, []network.CountFunc{
		p.readFunc,
	}
}

func (p *PacketConnCounter) UnwrapPacketWriter() (network.PacketWriter, []network.CountFunc) {
	return p.PacketConn, []network.CountFunc{
		p.writeFunc,
	}
}

func (p *PacketConnCounter) Upstream() any {
	return p.PacketConn
}

type PacketConnMultiCounter struct {
	network.PacketConn
	storages  []*TrafficStorage
	readFunc  network.CountFunc
	writeFunc network.CountFunc
}

func NewPacketConnMultiCounter(conn network.PacketConn, storages ...*TrafficStorage) network.PacketConn {
	counters := compactStorages(storages)
	return &PacketConnMultiCounter{
		PacketConn: conn,
		storages:   counters,
		readFunc: func(n int64) {
			addUp(counters, n)
		},
		writeFunc: func(n int64) {
			addDown(counters, n)
		},
	}
}

func (p *PacketConnMultiCounter) ReadPacket(buff *buf.Buffer) (destination M.Socksaddr, err error) {
	destination, err = p.PacketConn.ReadPacket(buff)
	if err != nil {
		return
	}
	if buff.Len() > 0 {
		addUp(p.storages, int64(buff.Len()))
	}
	return
}

func (p *PacketConnMultiCounter) WritePacket(buff *buf.Buffer, destination M.Socksaddr) (err error) {
	n := buff.Len()
	err = p.PacketConn.WritePacket(buff, destination)
	if err != nil {
		return
	}
	if n > 0 {
		addDown(p.storages, int64(n))
	}
	return
}

func (p *PacketConnMultiCounter) UnwrapPacketReader() (network.PacketReader, []network.CountFunc) {
	return p.PacketConn, []network.CountFunc{
		p.readFunc,
	}
}

func (p *PacketConnMultiCounter) UnwrapPacketWriter() (network.PacketWriter, []network.CountFunc) {
	return p.PacketConn, []network.CountFunc{
		p.writeFunc,
	}
}

func (p *PacketConnMultiCounter) Upstream() any {
	return p.PacketConn
}

func compactStorages(storages []*TrafficStorage) []*TrafficStorage {
	result := make([]*TrafficStorage, 0, len(storages))
	for _, storage := range storages {
		if storage != nil {
			result = append(result, storage)
		}
	}
	return result
}

func addUp(storages []*TrafficStorage, n int64) {
	if n <= 0 {
		return
	}
	for _, storage := range storages {
		storage.UpCounter.Add(n)
	}
}

func addDown(storages []*TrafficStorage, n int64) {
	if n <= 0 {
		return
	}
	for _, storage := range storages {
		storage.DownCounter.Add(n)
	}
}
