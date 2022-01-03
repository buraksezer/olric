package protocol

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProtocol_ParsePublishCommand(t *testing.T) {
	publishCmd := NewPublish("my-dtopic", "my-message")

	cmd := stringToCommand(publishCmd.Command(context.Background()).String())
	parsed, err := ParsePublishCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dtopic", parsed.Topic)
	require.Equal(t, "my-message", parsed.Message)
}

func TestProtocol_ParseSubscribeCommand(t *testing.T) {
	subscribeCmd := NewSubscribe("topic-1", "topic-2", "topic-3")

	cmd := stringToCommand(subscribeCmd.Command(context.Background()).String())
	parsed, err := ParseSubscribeCommand(cmd)
	require.NoError(t, err)

	topics := []string{"topic-1", "topic-2", "topic-3"}
	require.Equal(t, topics, parsed.Channels)
}
