import os
import json
import pytest
from unittest.mock import AsyncMock
from app.reindexer import Reindexer

@pytest.fixture
def temp_dirs(tmp_path):
    storage_dir = tmp_path / "storage"
    transcriptions_dir = tmp_path / "transcriptions"
    os.makedirs(storage_dir, exist_ok=True)
    os.makedirs(transcriptions_dir, exist_ok=True)
    return str(storage_dir), str(transcriptions_dir)

@pytest.fixture
def mock_nc():
    nc = AsyncMock()
    return nc

@pytest.mark.asyncio
async def test_reindexer_run(temp_dirs, mock_nc):
    storage_dir, transcriptions_dir = temp_dirs
    
    # Create an already transcribed file
    with open(os.path.join(storage_dir, "transcribed.m4a"), "w") as f:
        f.write("audio")
        
    # Create an untranscribed file
    with open(os.path.join(storage_dir, "untranscribed.m4a"), "w") as f:
        f.write("audio")
        
    # Create a non-audio file
    with open(os.path.join(storage_dir, "ignore.txt"), "w") as f:
        f.write("text")
        
    # Create transcript data for transcribed.m4a
    with open(os.path.join(transcriptions_dir, "transcripts_1.jsonl"), "w") as f:
        f.write(json.dumps({"original_file": "transcribed.m4a", "transcription": "test"}) + "\n")

    reindexer = Reindexer(
        storage_dir=storage_dir,
        transcriptions_dir=transcriptions_dir,
        nc=mock_nc,
        nats_subject="test.subject"
    )
    
    await reindexer.run()
    
    # Should only publish untranscribed.m4a
    assert mock_nc.publish.call_count == 1
    call_args = mock_nc.publish.call_args[0]
    assert call_args[0] == "test.subject"
    payload = json.loads(call_args[1].decode("utf-8"))
    
    assert payload["filename"] == "untranscribed.m4a"
    assert payload["out_of_order"] is True
