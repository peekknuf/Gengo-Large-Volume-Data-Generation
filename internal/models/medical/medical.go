package medical

import "time"

// --- Medical Model ---

// Patient represents the dim_patients table
type Patient struct {
	PatientID   int       `json:"patient_id" parquet:"patient_id"`
	PatientName string    `json:"patient_name" parquet:"patient_name"`
	DateOfBirth time.Time `json:"date_of_birth" parquet:"date_of_birth"`
	Gender      string    `json:"gender" parquet:"gender"`
}

// Doctor represents the dim_doctors table
type Doctor struct {
	DoctorID       int    `json:"doctor_id" parquet:"doctor_id"`
	DoctorName     string `json:"doctor_name" parquet:"doctor_name"`
	Specialization string `json:"specialization" parquet:"specialization"`
}

// Clinic represents the dim_clinics table
type Clinic struct {
	ClinicID   int    `json:"clinic_id" parquet:"clinic_id"`
	ClinicName string `json:"clinic_name" parquet:"clinic_name"`
	Address    string `json:"address" parquet:"address"`
}

// Appointment represents the fact_appointments table
type Appointment struct {
	AppointmentID   int64     `json:"appointment_id" parquet:"appointment_id"`
	PatientID       int       `json:"patient_id" parquet:"patient_id"`         // FK to dim_patients
	DoctorID        int       `json:"doctor_id" parquet:"doctor_id"`           // FK to dim_doctors
	ClinicID        int       `json:"clinic_id" parquet:"clinic_id"`           // FK to dim_clinics
	AppointmentDate time.Time `json:"appointment_date" parquet:"appointment_date"`
	Diagnosis       string    `json:"diagnosis" parquet:"diagnosis"`
}
