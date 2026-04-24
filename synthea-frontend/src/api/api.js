import axios from "axios"

/*For betterment we would use functions*/
const API_URL = import.meta.env.VITE_APP_API_URL || "/api"

export const generatePatients = async ({ numberOfPatients }) => {

	try {

		const resp = await axios.get(`${API_URL}/generate_data`, { params: { num_patients: numberOfPatients } })

		if (resp.status == 200)
			return resp.data

		return { message: `Error in generation`, cause: `${resp.status} --> ${resp.statusText}` }

	}
	catch (err) {

		return { message: `Error in patient generation`, reason: `${err}` };
	}
}

export const patientDashboard = async () => {

	try {
		const resp = await axios.get(`${API_URL}/patient_dashboard`)
		if (resp.status == 200)
			return { ...resp.data, success: true }

		return { message: 'Error in generation', reason: `${resp.status} --> ${resp.statusText}` }

	} catch (err) {
		return { message: 'Error in generating metrics', reason: `${err}` }
	}
}


export const conditionsDashboard = async () => {

	try {
		const resp = await axios.get(`${API_URL}/conditions_dashboard`)
		if (resp.status == 200)
			return { conditions_dashboard: resp.data }

		return { message: 'error in generation', reason: `${resp.status} --> ${resp.statusText}` }

	} catch (err) {
		return { message: 'error in generating metrics', reason: `${err}` }
	}
}

export const allergiesDashboard = async () => {

	try {
		const resp = await axios.get(`${API_URL}/allergies_dashboard`)
		if (resp.status == 200)
			return { allergies_dashboard: resp.data }

		return { message: 'error in generation', reason: `${resp.status} --> ${resp.statusText}` }

	} catch (err) {
		return { message: 'error in generating metrics', reason: `${err}` }
	}
}

export const encountersDashboard = async () => {

	try {
		const resp = await axios.get(`${API_URL}/encounters_dashboard`)
		if (resp.status == 200)
			return { encounters_dashboard: resp.data }

		return { message: 'error in generation', reason: `${resp.status} --> ${resp.statusText}` }

	} catch (err) {
		return { message: 'error in generating metrics', reason: `${err}` }
	}
}
