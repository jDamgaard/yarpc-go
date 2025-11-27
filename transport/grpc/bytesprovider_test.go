// Copyright (c) 2025 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package grpc

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/yarpc/api/peer/peertest"
	"go.uber.org/yarpc/api/transport"
	"google.golang.org/grpc"
)

type mockBytesProvider struct {
	*bytes.Reader
	data         []byte
	closed       bool
	bytesAccessed bool
}

func newMockBytesProvider(data []byte) *mockBytesProvider {
	return &mockBytesProvider{
		Reader: bytes.NewReader(data),
		data:   data,
	}
}

func (m *mockBytesProvider) Close() error {
	m.closed = true
	return nil
}

func (m *mockBytesProvider) Bytes() []byte {
	m.bytesAccessed = true
	return m.data
}

var _ transport.BytesProvider = (*mockBytesProvider)(nil)

type mockGrpcServerStream struct {
	grpc.ServerStream
	sentMessages [][]byte
}

func (m *mockGrpcServerStream) SendMsg(msg interface{}) error {
	if data, ok := msg.([]byte); ok {
		m.sentMessages = append(m.sentMessages, data)
	}
	return nil
}

func (m *mockGrpcServerStream) Context() context.Context {
	return context.Background()
}

type mockGrpcClientStream struct {
	grpc.ClientStream
	sentMessages [][]byte
}

func (m *mockGrpcClientStream) SendMsg(msg interface{}) error {
	if data, ok := msg.([]byte); ok {
		m.sentMessages = append(m.sentMessages, data)
	}
	return nil
}

func (m *mockGrpcClientStream) Context() context.Context {
	return context.Background()
}

func TestServerStreamSendMessageUsesBytesProvider(t *testing.T) {
	testData := []byte("test stream data")

	t.Run("uses BytesProvider when available", func(t *testing.T) {
		mockGrpcStream := &mockGrpcServerStream{}
		ss := &serverStream{
			ctx:    context.Background(),
			req:    &transport.StreamRequest{},
			stream: mockGrpcStream,
		}

		provider := newMockBytesProvider(testData)
		msg := &transport.StreamMessage{
			Body:     provider,
			BodySize: len(testData),
		}

		err := ss.SendMessage(context.Background(), msg)
		require.NoError(t, err)

		assert.True(t, provider.bytesAccessed, "BytesProvider.Bytes() should be called")
		assert.True(t, provider.closed, "Body should be closed")
		require.Len(t, mockGrpcStream.sentMessages, 1)
		assert.Equal(t, testData, mockGrpcStream.sentMessages[0])
	})

	t.Run("falls back to io.ReadAll when BytesProvider not available", func(t *testing.T) {
		mockGrpcStream := &mockGrpcServerStream{}
		ss := &serverStream{
			ctx:    context.Background(),
			req:    &transport.StreamRequest{},
			stream: mockGrpcStream,
		}

		msg := &transport.StreamMessage{
			Body:     io.NopCloser(bytes.NewReader(testData)),
			BodySize: len(testData),
		}

		err := ss.SendMessage(context.Background(), msg)
		require.NoError(t, err)

		require.Len(t, mockGrpcStream.sentMessages, 1)
		assert.Equal(t, testData, mockGrpcStream.sentMessages[0])
	})
}

func TestUnaryOutboundUsesBytesProvider(t *testing.T) {
	testData := []byte("unary request data")

	t.Run("uses BytesProvider when available", func(t *testing.T) {
		provider := newMockBytesProvider(testData)

		req := &transport.Request{
			Caller:    "caller",
			Service:   "service",
			Encoding:  transport.Encoding("raw"),
			Procedure: "hello",
			Body:      provider,
		}

		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()

		chooser := peertest.NewMockChooser(mockCtrl)
		mockPeer := peertest.NewMockPeer(mockCtrl)

		tran := NewTransport()
		out := newOutbound(tran, chooser)

		chooser.EXPECT().Start().Return(nil)
		chooser.EXPECT().Stop().Return(nil)
		chooser.EXPECT().IsRunning().Return(true).AnyTimes()
		chooser.EXPECT().Choose(gomock.Any(), gomock.Any()).Return(mockPeer, func(error) {}, nil)

		require.NoError(t, tran.Start())
		require.NoError(t, out.Start())
		defer tran.Stop()
		defer out.Stop()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := out.Call(ctx, req)

		assert.True(t, provider.bytesAccessed, "BytesProvider.Bytes() should be called")
		assert.NotNil(t, err)
	})
}

func TestClientStreamSendMessageUsesBytesProvider(t *testing.T) {
	testData := []byte("client stream data")

	t.Run("uses BytesProvider when available", func(t *testing.T) {
		mockGrpcStream := &mockGrpcClientStream{}
		cs := &clientStream{
			ctx:    context.Background(),
			req:    &transport.StreamRequest{},
			stream: mockGrpcStream,
		}

		provider := newMockBytesProvider(testData)
		msg := &transport.StreamMessage{
			Body:     provider,
			BodySize: len(testData),
		}

		err := cs.SendMessage(context.Background(), msg)
		require.NoError(t, err)

		assert.True(t, provider.bytesAccessed, "BytesProvider.Bytes() should be called")
		assert.True(t, provider.closed, "Body should be closed")
		require.Len(t, mockGrpcStream.sentMessages, 1)
		assert.Equal(t, testData, mockGrpcStream.sentMessages[0])
	})

	t.Run("falls back to io.ReadAll when BytesProvider not available", func(t *testing.T) {
		mockGrpcStream := &mockGrpcClientStream{}
		cs := &clientStream{
			ctx:    context.Background(),
			req:    &transport.StreamRequest{},
			stream: mockGrpcStream,
		}

		msg := &transport.StreamMessage{
			Body:     io.NopCloser(bytes.NewReader(testData)),
			BodySize: len(testData),
		}

		err := cs.SendMessage(context.Background(), msg)
		require.NoError(t, err)

		require.Len(t, mockGrpcStream.sentMessages, 1)
		assert.Equal(t, testData, mockGrpcStream.sentMessages[0])
	})
}

