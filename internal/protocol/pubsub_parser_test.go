package protocol

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProtocol_ParsePublishCommand(t *testing.T) {
	publishCmd := NewPublish("my-pubsub", "my-message")

	cmd := stringToCommand(publishCmd.Command(context.Background()).String())
	parsed, err := ParsePublishCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-pubsub", parsed.Channel)
	require.Equal(t, "my-message", parsed.Message)
}

func TestProtocol_ParseSubscribeCommand(t *testing.T) {
	subscribeCmd := NewSubscribe("channel-1", "channel-2", "channel-3")

	cmd := stringToCommand(subscribeCmd.Command(context.Background()).String())
	parsed, err := ParseSubscribeCommand(cmd)
	require.NoError(t, err)

	channels := []string{"channel-1", "channel-2", "channel-3"}
	require.Equal(t, channels, parsed.Channels)
}
