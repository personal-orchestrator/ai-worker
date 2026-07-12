import os
import json
import pytest
from unittest.mock import Mock, patch, AsyncMock
from app.workers.transcription import TranscriptionWorker
from app.services.transcription import TranscriptionResult, TranscriptionService

class MockTranscriptionService(TranscriptionService):
    def __init__(self, language="en"):
        self.language = language

    async def transcribe(self, audio_file_path: str) -> TranscriptionResult:
        return TranscriptionResult(text="This is a test transcription", language=self.language)

    async def translate(self, audio_file_path: str) -> TranscriptionResult:
        return TranscriptionResult(text="This is a translated test transcription", language="en")

@pytest.fixture
def mock_service():
    return MockTranscriptionService()

@pytest.fixture
def temp_dirs(tmp_path):
    storage_dir = tmp_path / "storage"
    transcriptions_raw_dir = tmp_path / "transcriptions-raw"
    transcriptions_dir = tmp_path / "transcriptions"
    return str(storage_dir), str(transcriptions_raw_dir), str(transcriptions_dir)

@pytest.fixture
def worker(mock_service, temp_dirs):
    storage_dir, transcriptions_raw_dir, transcriptions_dir = temp_dirs
    return TranscriptionWorker(
        transcription_service=mock_service,
        storage_dir=storage_dir,
        transcriptions_raw_dir=transcriptions_raw_dir,
        transcriptions_dir=transcriptions_dir
    )

@pytest.mark.asyncio
async def test_handle_message_success(worker, temp_dirs):
    storage_dir, transcriptions_raw_dir, transcriptions_dir = temp_dirs
    
    # Create a dummy audio file
    filename = "test_audio.m4a"
    file_path = os.path.join(storage_dir, filename)
    os.makedirs(storage_dir, exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(b"dummy data")

    # Create a dummy message
    msg = Mock()
    msg.subject = "audio.ingested"
    msg.data = json.dumps({"filename": filename}).encode("utf-8")
    msg.ack = AsyncMock()

    await worker.handle_message(msg)

    # Verify transcription output
    from datetime import datetime, timezone
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_filename = f"transcripts_{date_str}.jsonl"
    output_path = os.path.join(transcriptions_dir, output_filename)
    
    assert os.path.exists(output_path)
    with open(output_path, "r") as f:
        lines = f.readlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        
    assert data["transcription"] == "This is a test transcription"
    assert data["language"] == "en"
    assert data["original_file"] == filename
    assert "timestamp" in data

@pytest.mark.asyncio
async def test_handle_message_missing_file(worker, temp_dirs):
    storage_dir, transcriptions_raw_dir, transcriptions_dir = temp_dirs
    
    # Create a dummy message
    msg = Mock()
    msg.subject = "audio.ingested"
    msg.data = json.dumps({"filename": "missing.m4a"}).encode("utf-8")
    msg.ack = AsyncMock()

    await worker.handle_message(msg)

    # Output should not exist
    assert not os.listdir(transcriptions_dir)

@pytest.mark.asyncio
async def test_handle_message_invalid_json(worker, temp_dirs):
    storage_dir, transcriptions_raw_dir, transcriptions_dir = temp_dirs
    
    # Create a dummy message
    msg = Mock()
    msg.subject = "audio.ingested"
    msg.data = b"invalid json"
    msg.ack = AsyncMock()

    await worker.handle_message(msg)

    assert not os.listdir(transcriptions_dir)

def test_extract_timestamp(worker):
    # Valid timestamp in filename
    filename = "rec_1783484942586_4676c04a-e4bc-473b-ab31-43d5966a9be7.m4a"
    timestamp = worker._extract_timestamp(filename)
    assert timestamp == "2026-07-08T04:29:02.586000+00:00"

    # Invalid timestamp format (fallback to now, we just check it returns a string)
    assert isinstance(worker._extract_timestamp("invalid.m4a"), str)

@pytest.mark.asyncio
async def test_handle_message_reindex_sort(worker, temp_dirs):
    storage_dir, transcriptions_raw_dir, transcriptions_dir = temp_dirs
    os.makedirs(storage_dir, exist_ok=True)
    os.makedirs(transcriptions_dir, exist_ok=True)
    
    # Prepare existing jsonl file with out-of-order entries
    date_str = "2026-07-08"
    output_filename = f"transcripts_{date_str}.jsonl"
    output_path = os.path.join(transcriptions_dir, output_filename)
    
    with open(output_path, "w") as f:
        # A later timestamp
        f.write(json.dumps({"timestamp": "2026-07-08T12:00:00+00:00", "original_file": "old2.m4a", "transcription": "old"}) + "\n")
        # An earlier timestamp
        f.write(json.dumps({"timestamp": "2026-07-08T10:00:00+00:00", "original_file": "old1.m4a", "transcription": "old"}) + "\n")

    # Reindex file that corresponds to 2026-07-08T11:00:00+00:00
    # Let's mock _extract_timestamp or create a file with specific timestamp
    # 1783494000000 is 2026-07-08T07:00:00, let's just mock _extract_timestamp
    filename = "test_audio.m4a"
    file_path = os.path.join(storage_dir, filename)
    with open(file_path, "wb") as f:
        f.write(b"dummy")

    with patch.object(worker, '_extract_timestamp', return_value="2026-07-08T11:00:00+00:00"):
        msg = Mock()
        msg.subject = "audio.ingested"
        msg.data = json.dumps({"filename": filename, "out_of_order": True}).encode("utf-8")
        msg.ack = AsyncMock()
        
        await worker.handle_message(msg)
            
    with open(output_path, "r") as f:
        lines = f.readlines()
        
    assert len(lines) == 3
    data0 = json.loads(lines[0])
    data1 = json.loads(lines[1])
    data2 = json.loads(lines[2])
    
    assert data0["timestamp"] == "2026-07-08T10:00:00+00:00"
    assert data1["timestamp"] == "2026-07-08T11:00:00+00:00"
    assert data2["timestamp"] == "2026-07-08T12:00:00+00:00"

@pytest.mark.asyncio
async def test_handle_message_non_english(temp_dirs):
    storage_dir, transcriptions_raw_dir, transcriptions_dir = temp_dirs
    mock_service_pl = MockTranscriptionService(language="pl")
    
    worker_pl = TranscriptionWorker(
        transcription_service=mock_service_pl,
        storage_dir=storage_dir,
        transcriptions_raw_dir=transcriptions_raw_dir,
        transcriptions_dir=transcriptions_dir
    )
    
    filename = "test_audio_pl.m4a"
    file_path = os.path.join(storage_dir, filename)
    os.makedirs(storage_dir, exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(b"dummy data")

    msg = Mock()
    msg.subject = "audio.ingested"
    msg.data = json.dumps({"filename": filename}).encode("utf-8")
    msg.ack = AsyncMock()

    await worker_pl.handle_message(msg)

    # Verify both raw and translated directories contain output
    from datetime import datetime, timezone
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_filename = f"transcripts_{date_str}.jsonl"
    
    raw_path = os.path.join(transcriptions_raw_dir, output_filename)
    translated_path = os.path.join(transcriptions_dir, output_filename)
    
    assert os.path.exists(raw_path)
    assert os.path.exists(translated_path)
    
    with open(raw_path, "r") as f:
        raw_data = json.loads(f.readlines()[0])
        assert raw_data["transcription"] == "This is a test transcription"
        assert raw_data["language"] == "pl"
        
    with open(translated_path, "r") as f:
        translated_data = json.loads(f.readlines()[0])
        assert translated_data["transcription"] == "This is a translated test transcription"
        assert translated_data["language"] == "en"
