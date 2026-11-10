package relaymode

const (
	Unknown = iota
	ChatCompletions
	Completions
	Embeddings
	Moderations
	ImagesGenerations
	Edits
	ImagesEdits
	AudioSpeech
	AudioTranscription
	AudioTranslation
	// Proxy is a special relay mode for proxying requests to custom upstream
	Proxy
)
