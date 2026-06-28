from datetime import datetime, timedelta, timezone
from workflows.scripts.randomised_state_return import states as US_STATES

def parse_iso_datetime(dt_str):
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None

def calculate_total_visit_volume(records):
    result = {state: 0 for state in US_STATES}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if state in result:
            result[state] += 1
    return result

def calculate_total_revenue_generated(records):
    result = {state: 0.0 for state in US_STATES}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        fees = float(rec.get("visting_total_fees") or 0.0)
        if state in result:
            result[state] += fees
    for state in US_STATES:
        result[state] = round(result[state], 2)
    return result

def calculate_average_encounter_duration_hours(records):
    result = {state: 0.0 for state in US_STATES}
    state_hours = {}
    state_counts = {}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if not state:
            continue
        start = parse_iso_datetime(rec.get("visit_start"))
        end = parse_iso_datetime(rec.get("visit_end"))
        if start and end:
            dur_hours = (end - start).total_seconds() / 3600.0
            if state not in state_hours:
                state_hours[state] = 0.0
                state_counts[state] = 0
            state_hours[state] += dur_hours
            state_counts[state] += 1
            
    for state in US_STATES:
        tot_hours = state_hours.get(state, 0.0)
        cnt = state_counts.get(state, 0)
        if cnt > 0:
            result[state] = round(tot_hours / cnt, 2)
        else:
            result[state] = 0.0
    return result

def calculate_average_patient_out_of_pocket(records):
    result = {state: 0.0 for state in US_STATES}
    state_oop = {}
    state_counts = {}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if not state:
            continue
        fees = float(rec.get("visting_total_fees") or 0.0)
        coverage = float(rec.get("coverage") or 0.0)
        oop = max(0.0, fees - coverage)
        
        if state not in state_oop:
            state_oop[state] = 0.0
            state_counts[state] = 0
        state_oop[state] += oop
        state_counts[state] += 1
        
    for state in US_STATES:
        tot_oop = state_oop.get(state, 0.0)
        cnt = state_counts.get(state, 0)
        if cnt > 0:
            result[state] = round(tot_oop / cnt, 2)
        else:
            result[state] = 0.0
    return result

def calculate_average_base_fee(records):
    result = {state: 0.0 for state in US_STATES}
    state_base = {}
    state_counts = {}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if not state:
            continue
        base = float(rec.get("visiting_base_fees") or 0.0)
        if state not in state_base:
            state_base[state] = 0.0
            state_counts[state] = 0
        state_base[state] += base
        state_counts[state] += 1
        
    for state in US_STATES:
        tot_base = state_base.get(state, 0.0)
        cnt = state_counts.get(state, 0)
        if cnt > 0:
            result[state] = round(tot_base / cnt, 2)
        else:
            result[state] = 0.0
    return result

def calculate_total_covered_amount(records):
    result = {state: 0.0 for state in US_STATES}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        coverage = float(rec.get("coverage") or 0.0)
        if state in result:
            result[state] += coverage
    for state in US_STATES:
        result[state] = round(result[state], 2)
    return result

def calculate_unique_patients_seen(records):
    result = {state: 0 for state in US_STATES}
    state_patients = {state: set() for state in US_STATES}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        uuid = rec.get("uuid")
        if state in state_patients and uuid:
            state_patients[state].add(uuid)
    for state in US_STATES:
        result[state] = len(state_patients[state])
    return result

def calculate_average_practitioner_load(records):
    result = {state: 0.0 for state in US_STATES}
    state_prac_load = {} # state -> {practioner_id -> count}
    
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        prac_id = rec.get("practioner_id")
        if not state or not prac_id:
            continue
        if state not in state_prac_load:
            state_prac_load[state] = {}
        state_prac_load[state][prac_id] = state_prac_load[state].get(prac_id, 0) + 1
        
    for state in US_STATES:
        loads = state_prac_load.get(state, {})
        if loads:
            counts = list(loads.values())
            result[state] = round(sum(counts) / len(counts), 2)
        else:
            result[state] = 0.0
    return result

def calculate_encounters_30d(records):
    result = {state: 0 for state in US_STATES}
    now = datetime.now(timezone.utc)
    thirty_days_ago = now - timedelta(days=30)
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if not state:
            continue
        visit_start_str = rec.get("visit_start")
        visit_start = parse_iso_datetime(visit_start_str)
        if visit_start:
            if visit_start.tzinfo is None:
                visit_start = visit_start.replace(tzinfo=timezone.utc)
            if visit_start >= thirty_days_ago:
                if state in result:
                    result[state] += 1
    return result

async def get_state_encounter_kpis(kpi_id, supabase):
    try:
        response = await supabase.table("encounters").select(
            "visit_start, visit_end, visiting_base_fees, visting_total_fees, coverage, practioner_id, uuid, patients(geolocated_state)"
        ).execute()
        records = response.data or []
    except Exception as e:
        print(f"Error querying encounters table asynchronously: {e}")
        records = []

    # Initial filter on geolocated_state
    valid_records = [r for r in records if r.get("patients") and r.get("patients", {}).get("geolocated_state")]

    kpi_map = {
        "total_visit_volume": calculate_total_visit_volume,
        "total_revenue_generated": calculate_total_revenue_generated,
        "total_revenue": calculate_total_revenue_generated,
        "average_encounter_duration_hours": calculate_average_encounter_duration_hours,
        "average_patient_out_of_pocket": calculate_average_patient_out_of_pocket,
        "avg_out_of_pocket": calculate_average_patient_out_of_pocket,
        "average_base_fee": calculate_average_base_fee,
        "total_covered_amount": calculate_total_covered_amount,
        "insurer_covered": calculate_total_covered_amount,
        "unique_patients_seen": calculate_unique_patients_seen,
        "average_practitioner_load": calculate_average_practitioner_load,
        "encounters_30d": calculate_encounters_30d
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
