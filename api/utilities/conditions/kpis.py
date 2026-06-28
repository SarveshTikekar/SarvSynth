from datetime import datetime, timedelta, timezone
import math
from workflows.scripts.randomised_state_return import states as US_STATES

def parse_date(date_str):
    if not date_str:
        return None
    try:
        if "T" in date_str:
            date_str = date_str.split("T")[0]
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None

def calculate_active_condition_burden(records):
    result = {state: 0 for state in US_STATES}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if state in result:
            if not rec.get("date_of_abetment"):
                result[state] += 1
    return result

def calculate_global_recovery_rate(records):
    result = {state: 0.0 for state in US_STATES}
    state_total = {}
    state_resolved = {}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if not state:
            continue
        if state not in state_total:
            state_total[state] = 0
            state_resolved[state] = 0
        state_total[state] += 1
        if rec.get("date_of_abetment"):
            state_resolved[state] += 1
            
    for state in US_STATES:
        total = state_total.get(state, 0)
        resolved = state_resolved.get(state, 0)
        if total > 0:
            result[state] = round((resolved / total) * 100, 1)
        else:
            result[state] = 0.0
    return result

def calculate_patient_complexity_score(records):
    result = {state: 0 for state in US_STATES}
    state_patient_concepts = {} # state -> {uuid -> set(concepts)}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if not state or not rec.get("date_of_abetment"):
            continue
        uuid = rec.get("patients", {}).get("uuid") or rec.get("uuid") # wait, conditions could have a patient uuid field, or join uuid.
        concept = rec.get("medical_concepts")
        if not uuid or not concept:
            continue
        if state not in state_patient_concepts:
            state_patient_concepts[state] = {}
        if uuid not in state_patient_concepts[state]:
            state_patient_concepts[state][uuid] = set()
        state_patient_concepts[state][uuid].add(concept)
        
    for state in US_STATES:
        patient_dicts = state_patient_concepts.get(state, {})
        if patient_dicts:
            counts = [len(concepts) for concepts in patient_dicts.values()]
            avg_count = sum(counts) / len(counts)
            result[state] = math.ceil(avg_count)
        else:
            result[state] = 0
    return result

def calculate_avg_cure_time(records):
    result = {state: 0.0 for state in US_STATES}
    state_days = {}
    state_counts = {}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if not state:
            continue
        start_date = parse_date(rec.get("condition_record_date"))
        end_date = parse_date(rec.get("date_of_abetment"))
        if start_date and end_date:
            days = (end_date - start_date).days
            if state not in state_days:
                state_days[state] = 0.0
                state_counts[state] = 0
            state_days[state] += days
            state_counts[state] += 1
            
    for state in US_STATES:
        count = state_counts.get(state, 0)
        days = state_days.get(state, 0.0)
        if count > 0:
            result[state] = math.ceil(days / count)
        else:
            result[state] = 0.0
    return result

def calculate_admission_rate_last_30_days(records):
    result = {state: 0 for state in US_STATES}
    now = datetime.now()
    thirty_days_ago = now - timedelta(days=30)
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if state in result:
            rec_date = parse_date(rec.get("condition_record_date"))
            if rec_date and rec_date >= thirty_days_ago:
                result[state] += 1
    return result

def calculate_total_diagnoses(records):
    result = {state: 0 for state in US_STATES}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if state in result:
            result[state] += 1
    return result

def calculate_unique_conditions(records):
    result = {state: 0 for state in US_STATES}
    state_concepts = {state: set() for state in US_STATES}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        concept = rec.get("medical_concepts")
        if state in state_concepts and concept:
            state_concepts[state].add(concept)
    for state in US_STATES:
        result[state] = len(state_concepts[state])
    return result

def calculate_chronic_condition_burden(records):
    result = {state: 0 for state in US_STATES}
    now = datetime.now()
    ninety_days_ago = now - timedelta(days=90)
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if state in result:
            if not rec.get("date_of_abetment"):
                rec_date = parse_date(rec.get("condition_record_date"))
                if rec_date and rec_date <= ninety_days_ago:
                    result[state] += 1
    return result

async def get_state_condition_kpis(kpi_id, supabase):
    try:
        response = await supabase.table("conditions").select("date_of_abetment, condition_record_date, medical_concepts, uuid, patients(geolocated_state, uuid)").execute()
        records = response.data or []
    except Exception as e:
        print(f"Error querying conditions table asynchronously: {e}")
        records = []

    # Initial filter on geolocated_state
    valid_records = [r for r in records if r.get("patients") and r.get("patients", {}).get("geolocated_state")]

    kpi_map = {
        "active_condition_burden": calculate_active_condition_burden,
        "current_active_burden": calculate_active_condition_burden,
        "global_recovery_rate": calculate_global_recovery_rate,
        "patient_complexity_score": calculate_patient_complexity_score,
        "avg_cure_time": calculate_avg_cure_time,
        "average_time_to_cure": calculate_avg_cure_time,
        "admission_rate_last_30_days": calculate_admission_rate_last_30_days,
        "total_diagnoses": calculate_total_diagnoses,
        "unique_conditions": calculate_unique_conditions,
        "chronic_condition_burden": calculate_chronic_condition_burden
    }

    calc_func = kpi_map.get(kpi_id)
    if calc_func:
        vals = calc_func(valid_records)
    else:
        vals = {state: 0 for state in US_STATES}

    output = {}
    for state in US_STATES:
        output[state] = {
            "value": vals.get(state, 0),
            "trend": 0.0,
            "is_simulated": False
        }
    return output
