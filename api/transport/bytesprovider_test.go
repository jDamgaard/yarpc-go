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

package transport_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/yarpc/api/transport"
)

type testBytesProvider struct {
	*bytes.Reader
	data   []byte
	closed bool
}

func newTestBytesProvider(data []byte) *testBytesProvider {
	return &testBytesProvider{
		Reader: bytes.NewReader(data),
		data:   data,
	}
}

func (t *testBytesProvider) Close() error {
	t.closed = true
	return nil
}

func (t *testBytesProvider) Bytes() []byte {
	return t.data
}

var _ transport.BytesProvider = (*testBytesProvider)(nil)

func TestBytesProviderInterface(t *testing.T) {
	testData := []byte("test message data")

	t.Run("implements io.ReadCloser", func(t *testing.T) {
		provider := newTestBytesProvider(testData)

		var _ io.ReadCloser = provider

		readData, err := io.ReadAll(provider)
		require.NoError(t, err)
		assert.Equal(t, testData, readData)

		err = provider.Close()
		require.NoError(t, err)
		assert.True(t, provider.closed)
	})

	t.Run("provides direct byte access", func(t *testing.T) {
		provider := newTestBytesProvider(testData)

		bytes := provider.Bytes()
		assert.Equal(t, testData, bytes)
		assert.True(t, &testData[0] == &bytes[0], "Bytes() should return reference, not copy")
	})

	t.Run("type assertion works", func(t *testing.T) {
		provider := newTestBytesProvider(testData)

		var readCloser io.ReadCloser = provider

		bytesProvider, ok := readCloser.(transport.BytesProvider)
		assert.True(t, ok)
		assert.Equal(t, testData, bytesProvider.Bytes())
	})
}

func TestStreamMessageWithBytesProvider(t *testing.T) {
	testData := []byte("stream message content")

	t.Run("can be used in StreamMessage", func(t *testing.T) {
		provider := newTestBytesProvider(testData)

		msg := &transport.StreamMessage{
			Body:     provider,
			BodySize: len(testData),
		}

		assert.NotNil(t, msg.Body)
		assert.Equal(t, len(testData), msg.BodySize)

		bytesProvider, ok := msg.Body.(transport.BytesProvider)
		require.True(t, ok)
		assert.Equal(t, testData, bytesProvider.Bytes())
	})

	t.Run("can fallback to io.ReadAll", func(t *testing.T) {
		provider := newTestBytesProvider(testData)

		msg := &transport.StreamMessage{
			Body:     provider,
			BodySize: len(testData),
		}

		readData, err := io.ReadAll(msg.Body)
		require.NoError(t, err)
		assert.Equal(t, testData, readData)
	})
}
