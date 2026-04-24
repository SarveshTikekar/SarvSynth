from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import sys
import subprocess
import asyncio
from functools import wraps

# Ensuring the project root is in sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from api.supabase_builder import get_supabase_client

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)

# --- Global Supabase Client ---
_supabase_client = None

async def get_supabase():
    global _supabase_client
    if _supabase_client is None:
        # Import inside to avoid circular dependency if any
        from api.supabase_builder import get_supabase_client as init_client
        _supabase_client = await init_client()
    return _supabase_client

# --- Dependency Injection Decorator ---
def with_supabase(f):
    """
    Decorator to inject an async Supabase client into the route function.
    Handles the async lifecycle and ensures the current event loop is used.
    """
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        supabase = await get_supabase()
        return await f(supabase, *args, **kwargs)
    return decorated_function

async def fetch_metrics(supabase, entity_name):
    """
    Helper to fetch metrics using an injected client.
    """
    try:
        response = await supabase.table("metrics").select("*").eq("entity_name", entity_name).execute()
        metrics_dict = {}
        for row in response.data:
            metrics_dict[row['metric_type'].lower()] = row['data']
        return metrics_dict
    except Exception as e:
        print(f"Error fetching metrics for {entity_name}: {e}")
        return None

# --- ROUTES ---

@app.route('/api/', methods=['GET'])
async def root():
    return jsonify({'message': 'Welcome to SarvSynth API', 'status': 'OK'}), 200

@app.route('/api/patients', methods=['GET'])
@with_supabase
async def get_patients(supabase):
    """Return patients from Supabase"""
    try:
        limit = request.args.get('limit', default=100, type=int)
        response = await supabase.table("patients").select("*").limit(limit).execute()
        return jsonify(response.data), 200
    except Exception as e:
        return jsonify({'error': f'Failed to retrieve patients: {str(e)}'}), 500

@app.route('/api/get_patient_count', methods=['GET'])
@with_supabase
async def get_patient_count(supabase):
    """Return patient count from Supabase"""
    try:
        response = await supabase.table("patients").select("uuid", count="exact").execute()
        return jsonify({
            'status': 200,
            'patient_count': response.count,
            'message': 'Patient count returned successfully'
        })
    except Exception as e:
        return jsonify({"status": 500, "error": str(e)}), 500

@app.route('/api/generate_data/', methods=['GET'])
async def generate_data():
    """Trigger data generation script (No DB dependency here)"""
    try:
        num_patients = request.args.get('num_patients', default=10, type=int)
        script_path = os.path.join(project_root, "scripts", "synthea-init.sh")

        if not os.path.exists(script_path):
            return jsonify({"status": "error", "message": "Synthea script not found"}), 404

        await asyncio.to_thread(subprocess.run, [script_path, str(num_patients)], check=True)
        
        return jsonify({
            "status": "success",
            "message": f"{num_patients} patient records generation triggered"
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/patient_dashboard', methods=['GET'])
@with_supabase
async def patient_dashboard(supabase):
    """Fetch patient metrics from Supabase"""
    metrics = await fetch_metrics(supabase, "patients")
    if not metrics:
        return jsonify({'message': 'Data not found. Run analytics pipeline first.'}), 404
    
    adv_metrics = metrics.get('advanced_metrics', {})
    
    return jsonify({
        'message': 'Data Loaded successfully',
        'kpis': metrics.get('kpis', {}),
        'metrics': metrics.get('metrics', {}),
        'metric_trends': {
            'economic_dependence': adv_metrics.get('economic_dependence_trend', []),
            'cultural_diversity': adv_metrics.get('cultural_diversity_trend', []),
            'mortality_rate': adv_metrics.get('mortality_rate_trend', [])
        },
        'advanced_metrics': adv_metrics
    })

@app.route('/api/conditions_dashboard', methods=['GET'])
@with_supabase
async def conditions_dashboard(supabase):
    """Fetch condition metrics from Supabase"""
    metrics = await fetch_metrics(supabase, "conditions")
    if not metrics:
        return jsonify({'message': 'Data not found. Run analytics pipeline first.'}), 404
    
    return jsonify({
        'message': 'Data Loaded successfully',
        'kpis': metrics.get('kpis', {}),
        'metrics': metrics.get('metrics', {}),
        'advanced_metrics': metrics.get('advanced_metrics', {})
    })

@app.route('/api/encounters_dashboard', methods=['GET'])
@with_supabase
async def encounters_dashboard(supabase):
    """Fetch encounter metrics from Supabase"""
    metrics = await fetch_metrics(supabase, "encounters")
    if not metrics:
        return jsonify({'message': 'Data not found. Run analytics pipeline first.'}), 404
    
    return jsonify({
        'message': 'Data Loaded successfully',
        'kpis': metrics.get('kpis', {}),
        'metrics': metrics.get('metrics', {}),
        'advanced_metrics': metrics.get('advanced_metrics', {})
    })

@app.route('/api/quick_dashboard', methods=['GET'])
@with_supabase
async def quick_dashboard_data(supabase):
    """Legacy endpoint for raw patient data"""
    return await get_patients(supabase)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3001, debug=True)