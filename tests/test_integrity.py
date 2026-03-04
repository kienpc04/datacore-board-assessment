import pandas as pd
import os
import yaml

def test_golden_dataset_quality():
    # Load config để tìm file
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    with open(os.path.join(project_root, "config.yaml"), 'r') as f:
        config = yaml.safe_load(f)
    
    path = os.path.join(project_root, config['paths']['final_golden'])
    
    # Kiểm tra file tồn tại
    assert os.path.exists(path), "File board_golden.parquet chưa được tạo!"
    
    df = pd.read_parquet(path)
    
    # 1. Kiểm tra số lượng cột
    expected_columns = {'ticker', 'exchange', 'person_name', 'role', 'source_agreement', 'confidence_score'}
    assert expected_columns.issubset(set(df.columns))
    
    # 2. Kiểm tra Null (Không được có dòng Null ở các cột chính)
    assert df['ticker'].isnull().sum() == 0
    assert df['person_name'].isnull().sum() == 0
    assert df['confidence_score'].isnull().sum() == 0
    
    # 3. Kiểm tra dải điểm
    assert df['confidence_score'].min() >= 0
    assert df['confidence_score'].max() <= 1.0