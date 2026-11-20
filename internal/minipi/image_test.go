package minipi_test

import (
	"context"
	"reflect"
	"testing"

	. "ojj/internal/minipi"
)

func TestImagesSurviveRunAndContinueIsolation(t *testing.T) {
	image := Image{URL: "https://images.example/a.png", Label: "event 1"}
	input := Message{Role: RoleUser, Images: []Image{image}}
	agent := newAgent(t, Config{Model: modelFunc(func(_ context.Context, req Request, _ func(Message)) (Response, error) {
		if !reflect.DeepEqual(req.Messages[0].Images, []Image{image}) {
			t.Fatal("image lost before model")
		}
		req.Messages[0].Images[0].URL = "mutated"
		return answer("ok"), nil
	})})
	result, err := agent.Run(context.Background(), Context{}, []Message{input}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if input.Images[0] != image || result.Messages[0].Images[0] != image {
		t.Fatal("image slice aliased")
	}
	_, err = agent.Continue(context.Background(), Context{Messages: []Message{input}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if input.Images[0] != image {
		t.Fatal("continued image slice aliased")
	}
}

func TestImageValidation(t *testing.T) {
	for _, input := range []Message{
		{Role: RoleAssistant, Images: []Image{{URL: "https://example.org/a"}}},
		{Role: RoleTool, Images: []Image{{URL: "https://example.org/a"}}},
		{Role: RoleUser, Images: []Image{{URL: "https://example.org/a", Label: string([]byte{255})}}},
		{Role: RoleUser, Images: []Image{{URL: "https://example.org/a#fragment"}}},
	} {
		if ValidateImages(input.Role, input.Images) == nil {
			t.Fatal("invalid image accepted")
		}
	}
}
