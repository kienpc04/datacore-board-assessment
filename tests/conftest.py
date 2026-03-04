import pytest
import os
import sys

# Đảm bảo pytest tìm thấy thư mục src
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(project_root, "src"))

from merge import GoldenMerger

@pytest.fixture(scope="module")
def merger():
    """Khởi tạo instance GoldenMerger để dùng chung cho toàn bộ module test."""
    return GoldenMerger()