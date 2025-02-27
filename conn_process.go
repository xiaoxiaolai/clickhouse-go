// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package clickhouse

import (
	"context"
	"fmt"
	"github.com/xiaoxiaolai/clickhouse-go/lib/proto"
	"io"
)

type onProcess struct {
	data          func(*proto.Block)
	logs          func([]Log)
	progress      func(*Progress)
	profileInfo   func(*ProfileInfo)
	profileEvents func([]ProfileEvent)
}

func (c *connect) firstBlock(ctx context.Context, on *onProcess) (*proto.Block, error) {
	for {
		select {
		case <-ctx.Done():
			c.cancel()
			return nil, ctx.Err()
		default:
		}
		packet, err := c.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		switch packet {
		case proto.ServerData:
			return c.readData(packet, true)
		case proto.ServerEndOfStream:
			c.debugf("[end of stream]")
			return nil, io.EOF
		default:
			if err := c.handle(packet, on); err != nil {
				return nil, err
			}
		}
	}
}

func (c *connect) process(ctx context.Context, on *onProcess) error {
	for {
		select {
		case <-ctx.Done():
			c.cancel()
			return ctx.Err()
		default:
		}
		packet, err := c.reader.ReadByte()
		if err != nil {
			return err
		}
		switch packet {
		case proto.ServerEndOfStream:
			c.debugf("[end of stream]")
			return nil
		}
		if err := c.handle(packet, on); err != nil {
			return err
		}
	}
}

func (c *connect) handle(packet byte, on *onProcess) error {
	switch packet {
	case proto.ServerData, proto.ServerTotals, proto.ServerExtremes:
		block, err := c.readData(packet, true)
		if err != nil {
			return err
		}
		if block.Rows() != 0 && on.data != nil {
			on.data(block)
		}
	case proto.ServerException:
		return c.exception()
	case proto.ServerProfileInfo:
		var info proto.ProfileInfo
		if err := info.Decode(c.reader, c.revision); err != nil {
			return err
		}
		c.debugf("[profile info] %s", &info)
		on.profileInfo(&info)
	case proto.ServerTableColumns:
		var info proto.TableColumns
		if err := info.Decode(c.reader, c.revision); err != nil {
			return err
		}
		c.debugf("[table columns]")
	case proto.ServerProfileEvents:
		events, err := c.profileEvents()
		if err != nil {
			return err
		}
		on.profileEvents(events)
	case proto.ServerLog:
		logs, err := c.logs()
		if err != nil {
			return err
		}
		on.logs(logs)
	case proto.ServerProgress:
		progress, err := c.progress()
		if err != nil {
			return err
		}
		c.debugf("[progress] %s", progress)
		on.progress(progress)
	default:
		return &OpError{
			Op:  "process",
			Err: fmt.Errorf("unexpected packet %d", packet),
		}
	}
	return nil
}

func (c *connect) cancel() error {
	c.debugf("[cancel]")
	c.buffer.PutUVarInt(proto.ClientCancel)
	wErr := c.flush()
	// don't reuse a cancelled query as we don't drain the connection
	if cErr := c.close(); cErr != nil {
		return cErr
	}
	return wErr
}
