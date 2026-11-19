package amqp

import (
	"encoding/json"
	"testing"

	"github.com/cyverse-de/messaging/v12"
	"github.com/cyverse-de/model/v10"
)

// TestAnalysisUpdateMsgDecodesPublishedUpdate guards the wire contract between the job status
// pipeline, which publishes messaging.UpdateMessage values, and the trimmed-down struct recv decodes.
func TestAnalysisUpdateMsgDecodesPublishedUpdate(t *testing.T) {
	published := messaging.UpdateMessage{
		Job:     &model.Job{InvocationID: "07d1b2c4-1111-2222-3333-444455556666", CondorID: "condor-1"},
		State:   messaging.SucceededState,
		Message: "done",
		SentOn:  "1700000000000",
		Sender:  "10.0.0.1",
	}
	body, err := json.Marshal(published)
	if err != nil {
		t.Fatal(err)
	}

	var got analysisUpdateMsg
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}

	if got.Job.UUID != published.Job.InvocationID {
		t.Errorf("UUID = %q, want %q", got.Job.UUID, published.Job.InvocationID)
	}
	if got.Job.CondorID != published.Job.CondorID {
		t.Errorf("CondorID = %q, want %q", got.Job.CondorID, published.Job.CondorID)
	}
	if got.State != published.State {
		t.Errorf("State = %q, want %q", got.State, published.State)
	}
	if got.Message != published.Message || got.SentOn != published.SentOn || got.Sender != published.Sender {
		t.Errorf("decoded %+v, want fields from %+v", got, published)
	}
}
