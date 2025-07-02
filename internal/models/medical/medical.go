package medical

import "time"

type Patient struct {
	PatientID   int       `json:"patient_id" parquet:"patient_id"`
	PatientName string    `json:"patient_name" parquet:"patient_name"`
	DateOfBirth time.Time `json:"date_of_birth" parquet:"date_of_birth"`
	Gender      string    `json:"gender" parquet:"gender"`
}

type Doctor struct {
	DoctorID       int    `json:"doctor_id" parquet:"doctor_id"`
	DoctorName     string `json:"doctor_name" parquet:"doctor_name"`
	Specialization string `json:"specialization" parquet:"specialization"`
}

type Clinic struct {
	ClinicID   int    `json:"clinic_id" parquet:"clinic_id"`
	ClinicName string `json:"clinic_name" parquet:"clinic_name"`
	Address    string `json:"address" parquet:"address"`
}

type Appointment struct {
	AppointmentID   int64     `json:"appointment_id" parquet:"appointment_id"`
	PatientID       int       `json:"patient_id" parquet:"patient_id"`
	DoctorID        int       `json:"doctor_id" parquet:"doctor_id"`
	ClinicID        int       `json:"clinic_id" parquet:"clinic_id"`
	AppointmentDate time.Time `json:"appointment_date" parquet:"appointment_date"`
	Diagnosis       string    `json:"diagnosis" parquet:"diagnosis"`
}
