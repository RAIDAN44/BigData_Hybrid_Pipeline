from pathlib import Path
import json

VIVA = Path(__file__).resolve().parents[1]


def test_required_files_exist():
    required = [
        'backend/app.py', 'backend/demo_adapter.py', 'backend/requirements.txt',
        'content/content.json', 'frontend/index.html', 'frontend/styles.css',
        'frontend/app.js', 'run.ps1', 'README_VIVA.md',
    ]
    assert all((VIVA / p).exists() for p in required)


def test_content_contract():
    data = json.loads((VIVA / 'content/content.json').read_text(encoding='utf-8'))
    assert [x['id'] for x in data['requirements']] == [f'6.{i}' for i in range(1, 13)]
    assert len(data['cases']) >= 25
    assert len(data['presenter']) >= 20
    assert len(data['viva']) >= 45
    assert len(data['expected_columns']) == 17


def test_frontend_has_no_external_cdn():
    html = (VIVA / 'frontend/index.html').read_text(encoding='utf-8')
    js = (VIVA / 'frontend/app.js').read_text(encoding='utf-8')
    combined = (html + js).lower()
    assert 'cdnjs' not in combined
    assert 'unpkg.com' not in combined
    assert 'cdn.jsdelivr' not in combined


def test_demo_protects_official_collections():
    text = (VIVA / 'backend/demo_adapter.py').read_text(encoding='utf-8')
    for name in ['orders_raw', 'orders_validated', 'orders_quarantine']:
        assert name in text
    assert 'build_spark_environment' in text
    assert 'src.file_router' in text
