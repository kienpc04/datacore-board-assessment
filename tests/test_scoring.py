import pandas as pd

def test_perfect_match_score(merger):
    """Test trường hợp 2 nguồn khớp hoàn toàn tên và chức vụ."""
    row = pd.Series({
        'person_name_cf': 'Trần Hùng Huy',
        'person_name_vs': 'Trần Hùng Huy',
        'role_cf': 'Chủ tịch HĐQT',
        'role_vs': 'Chủ tịch HĐQT',
        '_merge': 'both'
    })
    label, score = merger.calculate_smart_score(row)
    assert label == "both"
    assert score == 1.0

def test_conflict_role_score(merger):
    """Test trường hợp khớp tên nhưng lệch chức vụ (Vẫn được điểm cao nhưng nhãn là conflict)."""
    row = pd.Series({
        'person_name_cf': 'Nguyễn Văn A',
        'person_name_vs': 'Nguyễn Văn A',
        'role_cf': 'Kế toán trưởng',
        'role_vs': 'Trưởng Ban kiểm soát',
        '_merge': 'both'
    })
    label, score = merger.calculate_smart_score(row)
    assert label == "conflict"
    # Điểm phải thấp hơn 1.0 nhưng đủ cao để nhận diện cùng 1 người (>0.7)
    assert 0.7 <= score < 1.0

def test_single_source_score(merger):
    """Test dữ liệu đơn nguồn (Only)."""
    row = pd.Series({
        'person_name_cf': 'Người Mới',
        'person_name_vs': None,
        'role_cf': 'Giám đốc',
        'role_vs': None,
        '_merge': 'left_only'
    })
    label, score = merger.calculate_smart_score(row)
    assert label == "cafef_only"
    # Điểm theo logic base_score (0.2) + completeness (0.1) = 0.3
    assert 0.2 <= score <= 0.4