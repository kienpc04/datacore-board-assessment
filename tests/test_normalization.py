import pytest

def test_normalize_name(merger):
    # Test xóa danh xưng và học hàm đứng đầu
    assert merger.normalize_name("Ông Trần Hùng Huy") == "tran hung huy"
    assert merger.normalize_name("Bà Lê Thị Thu Thủy") == "le thi thu thuy"
    assert merger.normalize_name("TS. Nguyễn Văn A") == "nguyen van a"
    assert merger.normalize_name("ThS Nguyễn Văn B") == "nguyen van b"
    
    # Test xử lý khoảng trắng và chữ hoa/thường
    assert merger.normalize_name("  NGUYEN   van a  ") == "nguyen van a"
    
    # Test tên không dấu
    assert merger.normalize_name("Tran Hung Huy") == "tran hung huy"

def test_normalize_role(merger):
    # Test chuẩn hóa chức danh về dạng viết tắt chuẩn (Viết liền theo logic src)
    assert merger.normalize_role("Chủ tịch Hội đồng quản trị") == "cthdqt"
    assert merger.normalize_role("Tổng Giám đốc") == "tgd"
    
    # CẬP NHẬT: Kỳ vọng là 'photgd' thay vì 'pho tgd'
    assert merger.normalize_role("Phó Tổng Giám đốc") == "photgd"
    
    # Test tính chất của chức danh kép (Sắp xếp theo alphabet sau dấu /)
    role1 = merger.normalize_role("TGĐ/TVHĐQT")
    role2 = merger.normalize_role("TVHĐQT/TGĐ")
    assert role1 == role2